use powersync_service::mongo::{ConversionOptions, DateRenderMode, raw_to_sqlite_row};
use std::{hint::black_box, time::Instant};

fn main() {
    const ITERATIONS: usize = 100_000;
    let options = ConversionOptions {
        date_render_mode: DateRenderMode::LegacyMilliseconds,
        fixed_booleans_in_json: false,
    };
    for length in [1024, 10240, 102400] {
        let bytes =
            bson::to_vec(&bson::doc! { "_id": "benchmark", "value": "x".repeat(length) }).unwrap();
        let start = Instant::now();
        for _ in 0..ITERATIONS {
            black_box(raw_to_sqlite_row(black_box(&bytes), options).unwrap());
        }
        println!(
            "{length}: {:.2} us",
            start.elapsed().as_secs_f64() * 1e6 / ITERATIONS as f64
        );
    }
}
