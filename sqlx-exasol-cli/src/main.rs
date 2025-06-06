use clap::Parser;
use console::style;
use sqlx_cli::Opt;
use sqlx_exasol_impl::any::DRIVER;

#[tokio::main]
async fn main() {
    // Checks for `--no-dotenv` before parsing.
    sqlx_cli::maybe_apply_dotenv();
    sqlx::any::install_drivers(&[DRIVER]).expect("driver installation failed");

    let opt = Opt::parse();

    // no special handling here
    if let Err(error) = sqlx_cli::run(opt).await {
        println!("{} {}", style("error:").bold().red(), error);
        std::process::exit(1);
    }
}
