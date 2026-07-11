pub mod engines;
pub mod services;

pub mod monte_carlo_var {
    tonic::include_proto!("monte_carlo_var");
}

pub mod options_pricing {
    tonic::include_proto!("options_pricing");
}

pub mod benchmark_models {
    tonic::include_proto!("benchmark_models");
}

pub mod order_book_arena {
    tonic::include_proto!("order_book_arena");
}
