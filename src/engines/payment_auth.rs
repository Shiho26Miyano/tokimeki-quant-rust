//! Deterministic fraud-scoring rule engine for the Payment Auth Arena.
//!
//! Rules are pure integer arithmetic (no floats) so Rust and Java produce
//! byte-identical decisions over the same synthetic transaction flow —
//! the same "shared binary format, compare implementations" pattern used
//! by the Order Book Arena.

pub const RECORD_LEN: usize = 32;

/// Fixed-point transaction record decoded from the shared 32-byte wire format.
#[derive(Clone, Copy)]
pub struct Transaction {
    pub seq: u64,
    pub account_id: u64,
    pub amount_cents: u32,
    pub merchant_category: u32, // MCC
    pub country_code: u16,
    pub geo_delta_km: u16,
    pub velocity_1h: u16,
    pub channel: u8, // 0=card_present, 1=online (card-not-present), 2=atm
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u32)]
pub enum Decision {
    Approve = 0,
    Review = 1,
    Decline = 2,
}

pub struct FraudDecision {
    pub seq: u64,
    pub risk_score: u32,
    pub decision: Decision,
    pub reason_mask: u32,
}

pub const REASON_VELOCITY: u32 = 1 << 0;
pub const REASON_GEO_JUMP: u32 = 1 << 1;
pub const REASON_HIGH_AMOUNT: u32 = 1 << 2;
pub const REASON_RISKY_MCC: u32 = 1 << 3;
pub const REASON_CNP_HIGH_VALUE: u32 = 1 << 4;

/// High-risk merchant category codes: crypto exchange, gambling, cash advance, wire transfer.
const RISKY_MCC: [u32; 4] = [6051, 7995, 6011, 4829];

pub struct RuleLimits {
    pub velocity_limit: u32,
    pub geo_delta_limit_km: u32,
    pub amount_limit_cents: u32,
}

pub fn parse_records(buf: &[u8]) -> Result<Vec<Transaction>, String> {
    if buf.len() % RECORD_LEN != 0 {
        return Err(format!(
            "transaction_flow length {} is not a multiple of {}",
            buf.len(),
            RECORD_LEN
        ));
    }
    let n = buf.len() / RECORD_LEN;
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let rec = &buf[i * RECORD_LEN..(i + 1) * RECORD_LEN];
        out.push(Transaction {
            seq: u64::from_le_bytes(rec[0..8].try_into().unwrap()),
            account_id: u64::from_le_bytes(rec[8..16].try_into().unwrap()),
            amount_cents: u32::from_le_bytes(rec[16..20].try_into().unwrap()),
            merchant_category: u32::from_le_bytes(rec[20..24].try_into().unwrap()),
            country_code: u16::from_le_bytes(rec[24..26].try_into().unwrap()),
            geo_delta_km: u16::from_le_bytes(rec[26..28].try_into().unwrap()),
            velocity_1h: u16::from_le_bytes(rec[28..30].try_into().unwrap()),
            channel: rec[30],
        });
    }
    Ok(out)
}

/// Score one transaction against the fixed rule set. Weighted sum, scaled 0-1000.
/// >=700 DECLINE, >=300 REVIEW, else APPROVE.
pub fn score(tx: &Transaction, limits: &RuleLimits) -> FraudDecision {
    let mut reason_mask = 0u32;
    let mut score: u32 = 0;

    if (tx.velocity_1h as u32) > limits.velocity_limit {
        reason_mask |= REASON_VELOCITY;
        score += 400;
    }
    if (tx.geo_delta_km as u32) > limits.geo_delta_limit_km {
        reason_mask |= REASON_GEO_JUMP;
        score += 400;
    }
    if tx.amount_cents > limits.amount_limit_cents {
        reason_mask |= REASON_HIGH_AMOUNT;
        score += 300;
    }
    if RISKY_MCC.contains(&tx.merchant_category) {
        reason_mask |= REASON_RISKY_MCC;
        score += 350;
    }
    if tx.channel == 1 && tx.amount_cents > limits.amount_limit_cents / 2 {
        reason_mask |= REASON_CNP_HIGH_VALUE;
        score += 300;
    }

    let risk_score = score.min(1000);
    let decision = if risk_score >= 700 {
        Decision::Decline
    } else if risk_score >= 300 {
        Decision::Review
    } else {
        Decision::Approve
    };

    FraudDecision {
        seq: tx.seq,
        risk_score,
        decision,
        reason_mask,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> RuleLimits {
        RuleLimits {
            velocity_limit: 5,
            geo_delta_limit_km: 500,
            amount_limit_cents: 50_000,
        }
    }

    #[test]
    fn clean_transaction_approves() {
        let tx = Transaction {
            seq: 1,
            account_id: 42,
            amount_cents: 1_000,
            merchant_category: 5411, // grocery
            country_code: 1,
            geo_delta_km: 5,
            velocity_1h: 1,
            channel: 0,
        };
        let d = score(&tx, &limits());
        assert_eq!(d.decision, Decision::Approve);
        assert_eq!(d.reason_mask, 0);
    }

    #[test]
    fn velocity_and_geo_jump_declines() {
        let tx = Transaction {
            seq: 2,
            account_id: 42,
            amount_cents: 1_000,
            merchant_category: 5411,
            country_code: 1,
            geo_delta_km: 900,
            velocity_1h: 10,
            channel: 0,
        };
        let d = score(&tx, &limits());
        assert_eq!(d.decision, Decision::Decline);
        assert_eq!(d.reason_mask, REASON_VELOCITY | REASON_GEO_JUMP);
    }

    #[test]
    fn risky_mcc_alone_only_reviews() {
        let tx = Transaction {
            seq: 3,
            account_id: 42,
            amount_cents: 1_000,
            merchant_category: 6051, // crypto exchange
            country_code: 1,
            geo_delta_km: 5,
            velocity_1h: 1,
            channel: 0,
        };
        let d = score(&tx, &limits());
        assert_eq!(d.decision, Decision::Review);
        assert_eq!(d.reason_mask, REASON_RISKY_MCC);
    }

    #[test]
    fn card_not_present_high_value_flags() {
        let tx = Transaction {
            seq: 4,
            account_id: 42,
            amount_cents: 40_000,
            merchant_category: 5411,
            country_code: 1,
            geo_delta_km: 5,
            velocity_1h: 1,
            channel: 1, // online
        };
        let d = score(&tx, &limits());
        assert_eq!(d.reason_mask, REASON_CNP_HIGH_VALUE);
        assert_eq!(d.decision, Decision::Review);
    }

    #[test]
    fn parse_records_roundtrip() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&7u64.to_le_bytes());
        buf.extend_from_slice(&99u64.to_le_bytes());
        buf.extend_from_slice(&12_345u32.to_le_bytes());
        buf.extend_from_slice(&5411u32.to_le_bytes());
        buf.extend_from_slice(&1u16.to_le_bytes());
        buf.extend_from_slice(&3u16.to_le_bytes());
        buf.extend_from_slice(&2u16.to_le_bytes());
        buf.push(0u8);
        buf.push(0u8); // pad to 32
        let recs = parse_records(&buf).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].seq, 7);
        assert_eq!(recs[0].account_id, 99);
        assert_eq!(recs[0].amount_cents, 12_345);
    }

    #[test]
    fn rejects_misaligned_buffer() {
        let buf = vec![0u8; RECORD_LEN - 1];
        assert!(parse_records(&buf).is_err());
    }
}
