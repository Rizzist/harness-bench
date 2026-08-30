//! `hbench <name>` full-suite shorthand.

#[tokio::main]
async fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let code = match ahrb::hbench::parse(&args) {
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
    };
    std::process::exit(code);
}
