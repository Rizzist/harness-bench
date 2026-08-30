//! AHRB command-line entry point.

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let code = match ahrb::cli::parse(&args) {
        Ok(command) => match ahrb::cli::execute(command).await {
            Ok(code) => code,
            Err(error) => {
                eprintln!("ahrb: {error}");
                2
            }
        },
        Err(error) => {
            eprintln!("ahrb: {error}");
            2
        }
    };
    std::process::exit(code);
}
