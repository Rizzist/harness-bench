//! AHRB command-line entry point.

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let code = match ahrb::cli::parse(&args) {
        Ok(command) => {
            let run_options = match &command {
                ahrb::cli::Command::Run(options) => Some(options.clone()),
                _ => None,
            };
            match ahrb::cli::execute(command).await {
                Ok(code) => code,
                Err(error) => {
                    if let Some(options) = run_options {
                        let output = &options.output;
                        eprintln!(
                            "ahrb: run aborted for manifest {}: {error}",
                            options.manifest.display()
                        );
                        if !output.join("report.json").is_file() {
                            eprintln!("ahrb: report.json was not written");
                        }
                        match ahrb::report::write_failure_diagnostic(
                            output,
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
    std::process::exit(code);
}
