//! Built-in reference harness entry point.

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let code = match ahrb::mock_harness::run(&args).await {
        Ok(code) => code,
        Err(error) => {
            eprintln!("ahrb-mock-harness: {error}");
            2
        }
    };
    std::process::exit(code);
}
