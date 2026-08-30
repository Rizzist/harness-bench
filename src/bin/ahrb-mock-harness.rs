//! Built-in reference harness entry point.

fn main() {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .max_blocking_threads(4)
        .thread_keep_alive(std::time::Duration::from_millis(100))
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("ahrb-mock-harness: failed to create runtime: {error}");
            std::process::exit(2);
        }
    };
    let args: Vec<String> = std::env::args().skip(1).collect();
    let code = match runtime.block_on(ahrb::mock_harness::run(&args)) {
        Ok(code) => code,
        Err(error) => {
            eprintln!("ahrb-mock-harness: {error}");
            2
        }
    };
    std::process::exit(code);
}
