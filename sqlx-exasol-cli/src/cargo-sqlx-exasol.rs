use clap::Parser;
use console::style;
use sqlx_cli::Opt;
use sqlx_exasol::any::DRIVER;

/// Cargo invokes this binary as `cargo-sqlx-exasol sqlx-exasol <args>`
#[derive(Parser, Debug)]
#[clap(bin_name = "cargo")]
enum Cli {
    SqlxExasol(Opt),
}

#[tokio::main]
async fn main() {
    sqlx_cli::maybe_apply_dotenv();
    sqlx_exasol::any::install_drivers(&[DRIVER]).expect("driver installation failed");
    let Cli::SqlxExasol(opt) = Cli::parse();

    if let Err(error) = sqlx_cli::run(opt).await {
        println!("{} {}", style("error:").bold().red(), error);
        std::process::exit(1);
    }
}
