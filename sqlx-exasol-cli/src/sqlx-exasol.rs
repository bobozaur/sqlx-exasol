use clap::Parser;
use console::style;
use sqlx_cli::Opt;
use sqlx_exasol::any::DRIVER;

#[tokio::main]
async fn main() {
    
    sqlx_cli::maybe_apply_dotenv();
    sqlx_exasol::any::install_drivers(&[DRIVER]).expect("driver installation failed");
    let opt = Opt::parse();

    if let Err(error) = sqlx_cli::run(opt).await {
        println!("{} {}", style("error:").bold().red(), error);
        std::process::exit(1);
    }
}
