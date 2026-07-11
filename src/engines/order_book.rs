//! 高性能限价订单簿 (Limit Order Book)
//!
//! 设计要点:
//!   - 价格档位: 稠密数组 (tick 索引直接当下标), 查最优价 O(1)
//!   - 每档订单队列: 侵入式双向链表, 节点存在 arena 里, prev/next 是 u32 索引
//!     -> cancel 是真正的 O(1) (不是 VecDeque 的 O(n) 中间删除)
//!     -> 全 safe Rust, 没有 raw pointer
//!   - 稳态零分配: arena 预分配 + free list 复用; trades 写进调用方的 buffer
//!
//! 复杂度:
//!   limit_order  撮合 O(成交笔数), 挂单 O(1)
//!   cancel       O(1)
//!   best price   O(1) 读取; 档位清空时需要扫描游标(摊销后极小, 见 NOTE)

use std::collections::HashMap;

pub type OrderId = u64;
pub type Qty = u32;
pub type TickIdx = u32;

/// 空索引哨兵
const NIL: u32 = u32::MAX;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum Side {
    Buy = 0,
    Sell = 1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Trade {
    pub maker_id: OrderId,
    pub taker_id: OrderId,
    pub price_idx: TickIdx,
    pub qty: Qty,
}

/// 32 字节, 一个 cache line 装 2 个
#[derive(Clone, Copy)]
struct OrderNode {
    id: OrderId,    // 8
    qty: Qty,       // 4  剩余数量
    price_idx: TickIdx, // 4
    prev: u32,      // 4  同价档位内的前一笔 (时间上更早)
    next: u32,      // 4  同价档位内的后一笔 (时间上更晚) / free list 的 next
    side: Side,     // 1  (+3 padding)
}

impl OrderNode {
    const EMPTY: Self = OrderNode {
        id: 0,
        qty: 0,
        price_idx: 0,
        prev: NIL,
        next: NIL,
        side: Side::Buy,
    };
}

/// 一个价格档位 = 一条 FIFO 链表 (head = 最早的单)
#[derive(Clone, Copy)]
struct PriceLevel {
    head: u32,
    tail: u32,
    total_qty: u64, // 冗余维护, 免得算档位深度要遍历链表
}

impl PriceLevel {
    const EMPTY: Self = PriceLevel {
        head: NIL,
        tail: NIL,
        total_qty: 0,
    };

    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.head == NIL
    }
}

pub struct OrderBook {
    arena: Vec<OrderNode>,
    free_head: u32,

    bid_levels: Vec<PriceLevel>,
    ask_levels: Vec<PriceLevel>,
    n_levels: usize,

    /// OrderId -> arena index
    /// NOTE: 如果 order id 是单调递增且稠密的(交易所通常是),
    ///       换成 Vec<u32> 直接下标能再省掉 hash 的开销和一次 cache miss。
    ///       std HashMap 用 SipHash, 热路径上建议换 FxHashMap / ahash。
    index: HashMap<OrderId, u32>,

    /// 最高买价的档位索引; -1 表示买盘为空
    best_bid: i64,
    /// 最低卖价的档位索引; n_levels 表示卖盘为空
    best_ask: i64,
}

impl OrderBook {
    /// n_levels: 价格档位总数 (通常覆盖 mid ± 几千个 tick 就够了)
    /// capacity: arena 预分配的订单节点数 (峰值挂单量, 一次性分配好)
    pub fn new(n_levels: usize, capacity: usize) -> Self {
        assert!(n_levels > 0 && capacity > 0);
        assert!(capacity < NIL as usize, "capacity 超出 u32 索引范围");

        // 预建 free list: 0 -> 1 -> 2 -> ... -> NIL
        let mut arena = vec![OrderNode::EMPTY; capacity];
        for i in 0..capacity {
            arena[i].next = if i + 1 < capacity { (i + 1) as u32 } else { NIL };
        }

        OrderBook {
            arena,
            free_head: 0,
            bid_levels: vec![PriceLevel::EMPTY; n_levels],
            ask_levels: vec![PriceLevel::EMPTY; n_levels],
            n_levels,
            index: HashMap::with_capacity(capacity * 2), // 留够余量, 避免运行时 rehash
            best_bid: -1,
            best_ask: n_levels as i64,
        }
    }

    // ---------- arena 管理 ----------

    #[inline]
    fn alloc_node(&mut self) -> u32 {
        let idx = self.free_head;
        assert!(idx != NIL, "arena 耗尽 —— 调大 capacity");
        self.free_head = self.arena[idx as usize].next;
        idx
    }

    #[inline]
    fn free_node(&mut self, idx: u32) {
        self.arena[idx as usize].next = self.free_head;
        self.free_head = idx;
    }

    // ---------- 链表操作 (关联函数, 显式借用, 绕开 borrowck) ----------

    /// 挂到档位队尾 (时间优先: 后来的排后面)
    #[inline]
    fn push_back(arena: &mut [OrderNode], level: &mut PriceLevel, idx: u32) {
        let node = &mut arena[idx as usize];
        node.prev = level.tail;
        node.next = NIL;
        let qty = node.qty;

        if level.tail != NIL {
            arena[level.tail as usize].next = idx;
        } else {
            level.head = idx;
        }
        level.tail = idx;
        level.total_qty += qty as u64;
    }

    /// O(1) 摘除 —— 这就是侵入式链表的全部意义
    #[inline]
    fn unlink(arena: &mut [OrderNode], level: &mut PriceLevel, idx: u32) {
        let node = arena[idx as usize];

        if node.prev != NIL {
            arena[node.prev as usize].next = node.next;
        } else {
            level.head = node.next;
        }
        if node.next != NIL {
            arena[node.next as usize].prev = node.prev;
        } else {
            level.tail = node.prev;
        }
        level.total_qty -= node.qty as u64;
    }

    // ---------- 最优价游标维护 ----------
    //
    // NOTE: 档位清空后线性扫描找下一个非空档位。
    // 真实市场最优价附近的档位是稠密的, 一次通常只走 1~3 步, 摊销开销可以忽略。
    // 想要严格 O(1) 的话: 加一层 bitmap (u64 位图 + 硬件 ctz/clz 指令),
    // 4096 档只需要 64 个 u64, 一条 `trailing_zeros()` 就找到下一个非空档位。

    #[inline]
    fn rescan_best_ask(&mut self, from: usize) {
        let mut i = from;
        while i < self.n_levels && self.ask_levels[i].is_empty() {
            i += 1;
        }
        self.best_ask = i as i64;
    }

    #[inline]
    fn rescan_best_bid(&mut self, from: i64) {
        let mut i = from;
        while i >= 0 && self.bid_levels[i as usize].is_empty() {
            i -= 1;
        }
        self.best_bid = i;
    }

    // ---------- 公开 API ----------

    #[inline]
    pub fn best_bid(&self) -> Option<TickIdx> {
        (self.best_bid >= 0).then(|| self.best_bid as TickIdx)
    }

    #[inline]
    pub fn best_ask(&self) -> Option<TickIdx> {
        ((self.best_ask as usize) < self.n_levels).then(|| self.best_ask as TickIdx)
    }

    pub fn level_qty(&self, side: Side, price_idx: TickIdx) -> u64 {
        match side {
            Side::Buy => self.bid_levels[price_idx as usize].total_qty,
            Side::Sell => self.ask_levels[price_idx as usize].total_qty,
        }
    }

    /// 只读: 返回最优价起最多 `depth` 个非空档位的 (price_idx, total_qty),
    /// 按最优价优先排序 (bids: 价格从高到低; asks: 价格从低到高)。
    /// 用于生成深度快照, 不触碰撮合逻辑。
    pub fn top_levels(&self, side: Side, depth: usize) -> Vec<(TickIdx, u64)> {
        let mut out = Vec::with_capacity(depth);
        match side {
            Side::Buy => {
                let mut i = self.best_bid;
                while i >= 0 && out.len() < depth {
                    let lvl = &self.bid_levels[i as usize];
                    if !lvl.is_empty() {
                        out.push((i as TickIdx, lvl.total_qty));
                    }
                    i -= 1;
                }
            }
            Side::Sell => {
                let mut i = self.best_ask;
                while (i as usize) < self.n_levels && out.len() < depth {
                    let lvl = &self.ask_levels[i as usize];
                    if !lvl.is_empty() {
                        out.push((i as TickIdx, lvl.total_qty));
                    }
                    i += 1;
                }
            }
        }
        out
    }

    /// 限价单: 先撮合, 剩余部分挂单。
    /// trades 由调用方传入并复用 —— 热路径零分配。
    /// 返回挂入订单簿的剩余数量 (0 = 全部成交)。
    pub fn limit_order(
        &mut self,
        id: OrderId,
        side: Side,
        price_idx: TickIdx,
        qty: Qty,
        trades: &mut Vec<Trade>,
    ) -> Qty {
        debug_assert!((price_idx as usize) < self.n_levels);
        let remaining = self.match_against(id, side, Some(price_idx), qty, trades);

        if remaining > 0 {
            let node_idx = self.alloc_node();
            self.arena[node_idx as usize] = OrderNode {
                id,
                qty: remaining,
                price_idx,
                prev: NIL,
                next: NIL,
                side,
            };

            let lvl = price_idx as usize;
            match side {
                Side::Buy => {
                    Self::push_back(&mut self.arena, &mut self.bid_levels[lvl], node_idx);
                    if (lvl as i64) > self.best_bid {
                        self.best_bid = lvl as i64;
                    }
                }
                Side::Sell => {
                    Self::push_back(&mut self.arena, &mut self.ask_levels[lvl], node_idx);
                    if (lvl as i64) < self.best_ask {
                        self.best_ask = lvl as i64;
                    }
                }
            }
            self.index.insert(id, node_idx);
        }
        remaining
    }

    /// 市价单: 吃到多少算多少, 不挂单。返回未成交数量。
    pub fn market_order(
        &mut self,
        id: OrderId,
        side: Side,
        qty: Qty,
        trades: &mut Vec<Trade>,
    ) -> Qty {
        self.match_against(id, side, None, qty, trades)
    }

    /// O(1) 撤单。返回 false 表示订单不存在(已成交/已撤/根本没下过)。
    pub fn cancel(&mut self, id: OrderId) -> bool {
        let Some(idx) = self.index.remove(&id) else {
            return false;
        };
        let node = self.arena[idx as usize];
        let lvl = node.price_idx as usize;

        let emptied = match node.side {
            Side::Buy => {
                Self::unlink(&mut self.arena, &mut self.bid_levels[lvl], idx);
                self.bid_levels[lvl].is_empty()
            }
            Side::Sell => {
                Self::unlink(&mut self.arena, &mut self.ask_levels[lvl], idx);
                self.ask_levels[lvl].is_empty()
            }
        };

        // 只有清空的恰好是最优档才需要重扫游标
        if emptied {
            match node.side {
                Side::Buy if lvl as i64 == self.best_bid => self.rescan_best_bid(lvl as i64 - 1),
                Side::Sell if lvl as i64 == self.best_ask => self.rescan_best_ask(lvl + 1),
                _ => {}
            }
        }

        self.free_node(idx);
        true
    }

    // ---------- 撮合核心 ----------

    /// limit_price = None 表示市价单 (吃穿整个对手盘也不停)
    fn match_against(
        &mut self,
        taker_id: OrderId,
        taker_side: Side,
        limit_price: Option<TickIdx>,
        mut remaining: Qty,
        trades: &mut Vec<Trade>,
    ) -> Qty {
        while remaining > 0 {
            // 1. 取对手方最优档, 判断是否可撮合
            let lvl: usize = match taker_side {
                Side::Buy => {
                    let ask = self.best_ask;
                    if ask as usize >= self.n_levels {
                        break; // 卖盘空了
                    }
                    // 买单: 出价必须 >= 最优卖价
                    if let Some(p) = limit_price {
                        if (p as i64) < ask {
                            break;
                        }
                    }
                    ask as usize
                }
                Side::Sell => {
                    let bid = self.best_bid;
                    if bid < 0 {
                        break; // 买盘空了
                    }
                    // 卖单: 要价必须 <= 最优买价
                    if let Some(p) = limit_price {
                        if (p as i64) > bid {
                            break;
                        }
                    }
                    bid as usize
                }
            };

            // 2. 在该档位内按 FIFO 逐笔吃
            let levels: &mut Vec<PriceLevel> = match taker_side {
                Side::Buy => &mut self.ask_levels,
                Side::Sell => &mut self.bid_levels,
            };

            while remaining > 0 {
                let head = levels[lvl].head;
                if head == NIL {
                    break;
                }

                let maker = &mut self.arena[head as usize];
                let traded = remaining.min(maker.qty);
                let maker_id = maker.id;

                maker.qty -= traded;
                let maker_filled = maker.qty == 0;

                remaining -= traded;
                levels[lvl].total_qty -= traded as u64;

                trades.push(Trade {
                    maker_id,
                    taker_id,
                    price_idx: lvl as TickIdx, // 成交价 = maker 的挂单价
                    qty: traded,
                });

                if maker_filled {
                    // 从队首摘除 (unlink 会再减一次 total_qty, 此时 node.qty 已是 0, 无副作用)
                    Self::unlink(&mut self.arena, &mut levels[lvl], head);
                    self.index.remove(&maker_id);
                    // 手动 free (借用冲突, 不能直接调 self.free_node)
                    self.arena[head as usize].next = self.free_head;
                    self.free_head = head;
                }
            }

            // 3. 该档吃空 -> 推进游标
            let emptied = levels[lvl].is_empty();
            if emptied {
                match taker_side {
                    Side::Buy => self.rescan_best_ask(lvl + 1),
                    Side::Sell => self.rescan_best_bid(lvl as i64 - 1),
                }
            } else {
                // 档位没吃空 说明 remaining 已经是 0 了
                debug_assert_eq!(remaining, 0);
                break;
            }
        }
        remaining
    }
}

// =====================================================================
//  正确性测试 —— 性能数字有意义的前提是两边行为完全一致
// =====================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn book() -> OrderBook {
        OrderBook::new(1000, 10_000)
    }

    #[test]
    fn resting_and_best_price() {
        let mut b = book();
        let mut t = Vec::new();

        b.limit_order(1, Side::Buy, 100, 200, &mut t);
        b.limit_order(2, Side::Buy, 104, 500, &mut t);
        b.limit_order(3, Side::Sell, 107, 150, &mut t);

        assert!(t.is_empty(), "不该有成交");
        assert_eq!(b.best_bid(), Some(104));
        assert_eq!(b.best_ask(), Some(107));
    }

    #[test]
    fn price_time_priority() {
        let mut b = book();
        let mut t = Vec::new();

        // 同价 107, 三笔卖单, 时间顺序 10 -> 11 -> 12
        b.limit_order(10, Side::Sell, 107, 50, &mut t);
        b.limit_order(11, Side::Sell, 107, 50, &mut t);
        b.limit_order(12, Side::Sell, 107, 50, &mut t);
        // 更差的价格
        b.limit_order(13, Side::Sell, 108, 100, &mut t);
        t.clear();

        // 买 120 @ 108 -> 应先吃光 107 档 (按 10,11,12 顺序), 再吃 108
        let rest = b.limit_order(20, Side::Buy, 108, 120, &mut t);
        assert_eq!(rest, 0);
        assert_eq!(
            t,
            vec![
                Trade { maker_id: 10, taker_id: 20, price_idx: 107, qty: 50 },
                Trade { maker_id: 11, taker_id: 20, price_idx: 107, qty: 50 },
                Trade { maker_id: 12, taker_id: 20, price_idx: 107, qty: 20 },
            ]
        );
        // 12 还剩 30 挂着
        assert_eq!(b.best_ask(), Some(107));
        assert_eq!(b.level_qty(Side::Sell, 107), 30);
    }

    #[test]
    fn partial_fill_rests() {
        let mut b = book();
        let mut t = Vec::new();

        b.limit_order(1, Side::Sell, 100, 50, &mut t);
        t.clear();

        let rest = b.limit_order(2, Side::Buy, 100, 80, &mut t);
        assert_eq!(rest, 30);
        assert_eq!(t.len(), 1);
        assert_eq!(b.best_ask(), None);          // 卖盘吃空
        assert_eq!(b.best_bid(), Some(100));     // 剩余 30 挂成买单
        assert_eq!(b.level_qty(Side::Buy, 100), 30);
    }

    #[test]
    fn cancel_middle_of_queue() {
        let mut b = book();
        let mut t = Vec::new();

        b.limit_order(1, Side::Sell, 100, 10, &mut t);
        b.limit_order(2, Side::Sell, 100, 20, &mut t); // 队列中间
        b.limit_order(3, Side::Sell, 100, 30, &mut t);
        t.clear();

        assert!(b.cancel(2));
        assert!(!b.cancel(2), "重复撤单应返回 false");
        assert_eq!(b.level_qty(Side::Sell, 100), 40);

        b.limit_order(9, Side::Buy, 100, 40, &mut t);
        assert_eq!(
            t,
            vec![
                Trade { maker_id: 1, taker_id: 9, price_idx: 100, qty: 10 },
                Trade { maker_id: 3, taker_id: 9, price_idx: 100, qty: 30 },
            ],
            "2 已撤, 应直接跳过"
        );
    }

    #[test]
    fn cancel_best_level_moves_cursor() {
        let mut b = book();
        let mut t = Vec::new();

        b.limit_order(1, Side::Sell, 100, 10, &mut t);
        b.limit_order(2, Side::Sell, 105, 10, &mut t);
        assert_eq!(b.best_ask(), Some(100));

        b.cancel(1);
        assert_eq!(b.best_ask(), Some(105), "最优档撤空后游标要推进");

        b.cancel(2);
        assert_eq!(b.best_ask(), None);
    }

    #[test]
    fn market_order_sweeps() {
        let mut b = book();
        let mut t = Vec::new();

        b.limit_order(1, Side::Sell, 100, 10, &mut t);
        b.limit_order(2, Side::Sell, 101, 10, &mut t);
        b.limit_order(3, Side::Sell, 102, 10, &mut t);
        t.clear();

        let unfilled = b.market_order(9, Side::Buy, 25, &mut t);
        assert_eq!(unfilled, 0);
        assert_eq!(t.len(), 3);
        assert_eq!(t[2], Trade { maker_id: 3, taker_id: 9, price_idx: 102, qty: 5 });

        // 吃穿整个簿
        t.clear();
        let unfilled = b.market_order(10, Side::Buy, 100, &mut t);
        assert_eq!(unfilled, 95, "簿子空了, 剩下的直接丢弃");
        assert_eq!(b.best_ask(), None);
    }

    #[test]
    fn arena_is_reused() {
        // 反复下单+撤单, arena 必须复用节点而不是泄漏
        let mut b = OrderBook::new(100, 4); // 故意只给 4 个槽
        let mut t = Vec::new();
        for i in 0..1000u64 {
            b.limit_order(i, Side::Buy, 50, 1, &mut t);
            assert!(b.cancel(i));
        }
        assert_eq!(b.best_bid(), None);
    }

    #[test]
    fn no_alloc_in_steady_state() {
        // trades buffer 一次 reserve 好, 后续复用 -> 热路径不碰 allocator
        let mut b = book();
        let mut t = Vec::with_capacity(64);
        for i in 0..500u64 {
            t.clear();
            b.limit_order(i * 2, Side::Sell, 100, 5, &mut t);
            b.limit_order(i * 2 + 1, Side::Buy, 100, 5, &mut t);
            assert_eq!(t.capacity(), 64, "trades buffer 不该重新分配");
        }
    }
}
