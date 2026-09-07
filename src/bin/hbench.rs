//! `hbench <name>` full-suite shorthand.

#[tokio::main]
async fn main() {
    ahrb::process::install_cleanup_handlers();
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let code = if args.first().is_some_and(|argument| argument == "diff") {
        match ahrb::diff::execute(&args[1..]) {
            Ok(code) => code,
            Err(error) => {
                eprintln!("hbench: {error}");
                2
            }
        }
    } else if args.first().is_some_and(|argument| argument == "results") {
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
        type PillarParser = fn(&[String]) -> ahrb::Result<ahrb::hbench::Options>;
        let (parse, execute): (PillarParser, _) = match args.first().map(String::as_str) {
            Some("economy") => (ahrb::hbench::parse_economy, "economy"),
            Some("fidelity") => (ahrb::hbench::parse_fidelity, "fidelity"),
            Some("storage") => (ahrb::hbench::parse_storage, "storage"),
            _ => (ahrb::hbench::parse, "matrix"),
        };
        let arguments = if execute == "matrix" {
            &args[..]
        } else {
            &args[1..]
        };
        let result = match parse(arguments) {
            Ok(options) => match execute {
                "economy" => ahrb::hbench::execute_economy(options).await,
                "fidelity" => ahrb::hbench::execute_fidelity(options).await,
                "storage" => ahrb::hbench::execute_storage(options).await,
                _ => ahrb::hbench::execute(options).await,
            },
            Err(error) => Err(error),
        };
        match result {
            Ok(code) => code,
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
