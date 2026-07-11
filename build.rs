fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/monte_carlo_var.proto")?;
    tonic_build::compile_protos("proto/options_pricing.proto")?;
    tonic_build::compile_protos("proto/benchmark_models.proto")?;
    tonic_build::compile_protos("proto/order_book_arena.proto")?;

    Ok(())
}
