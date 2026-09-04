//! AHRB command-line entry point.

#[tokio::main]
async fn main() {
    ahrb::process::install_cleanup_handlers();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let code = match ahrb::cli::parse(&args) {
        Ok(command) => {
            let run_options = match &command {
                ahrb::cli::Command::Run(options)
                | ahrb::cli::Command::Economy(options)
                | ahrb::cli::Command::Fidelity(options) => Some(options.clone()),
                _ => None,
            };
            match ahrb::cli::execute(command).await {
                Ok(code) => code,
                Err(error) => {
                    if let Some(options) = run_options {
                        let output = if options.output.as_os_str().is_empty() {
                            ahrb::manifest::load(&options.manifest)
                                .and_then(|manifest| ahrb::results::prepare(&options, &manifest))
                                .map(|persistence| persistence.output)
                                .unwrap_or_else(|_| {
                                    std::path::PathBuf::from("ahrb-output/run-error")
                                })
                        } else {
                            options.output.clone()
                        };
                        eprintln!(
                            "ahrb: run aborted for manifest {}: {error}",
                            options.manifest.display()
                        );
                        if !output.join("report.json").is_file() {
                            eprintln!("ahrb: report.json was not written");
                        }
                        match ahrb::report::write_failure_diagnostic(
                            &output,
                            &options.manifest,
                            &error,
                        ) {
                            Ok(path) => {
                                eprintln!("ahrb: diagnostic written to {}", path.display());
                            }
                            Err(diagnostic_error) => {
                                eprintln!(
                                    "ahrb: could not write run-error.txt: {diagnostic_error}"
                                );
                            }
                        }
                    } else {
                        eprintln!("ahrb: {error}");
                    }
                    2
                }
            }
        }
        Err(error) => {
            eprintln!("ahrb: {error}");
            2
        }
    };
    let code = match ahrb::process::cleanup_owned_processes(std::time::Duration::from_millis(500)) {
        Ok(survivors) if survivors.is_empty() => code,
        Ok(survivors) => {
            eprintln!("ahrb: final cleanup left owned processes alive: {survivors:?}");
            2
        }
        Err(error) => {
            eprintln!("ahrb: final owned-process cleanup failed: {error}");
            2
        }
    };
    std::process::exit(code);
}
