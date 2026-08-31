//! `hbench <name>` full-suite shorthand.

#[tokio::main]
async fn main() {
    ahrb::process::install_cleanup_handlers();
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let code = if args.first().is_some_and(|argument| argument == "results") {
        match ahrb::cli::parse(&args) {
            Ok(command) => match ahrb::cli::execute(command).await {
                Ok(code) => code,
                Err(error) => {
                    eprintln!("hbench: {error}");
                    2
                }
            },
            Err(error) => {
                eprintln!("hbench: {error}");
                2
            }
        }
    } else {
        match ahrb::hbench::parse(&args) {
            Ok(options) => match ahrb::hbench::execute(options).await {
                Ok(code) => code,
                Err(error) => {
                    eprintln!("hbench: {error}");
                    2
                }
            },
            Err(error) => {
                eprintln!("hbench: {error}");
                2
            }
        }
    };
    let code = match ahrb::process::cleanup_owned_processes(std::time::Duration::from_millis(500)) {
        Ok(survivors) if survivors.is_empty() => code,
        Ok(survivors) => {
            eprintln!("hbench: final cleanup left owned processes alive: {survivors:?}");
            2
        }
        Err(error) => {
            eprintln!("hbench: final owned-process cleanup failed: {error}");
            2
        }
    };
    std::process::exit(code);
}
