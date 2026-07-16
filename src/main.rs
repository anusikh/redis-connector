//! CLI binary for the Redis CDC connector.
//!
//! Usage:
//!   ./redis-connector cdc-producer
//!   ./redis-connector cdc-consumer

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "redis-connector", about = "PostgreSQL -> Redis CDC connector")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the CDC producer (WAL parsing -> Redis streams).
    CdcProducer,
    /// Run the CDC consumer worker (Redis stream consumer groups).
    CdcConsumer,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::CdcProducer => {
            let cfg = cdc_producer::ProducerConfig::from_env();
            cdc_producer::run(cfg).await?;
        }
        Commands::CdcConsumer => {
            let cfg = cdc_consumer::ConsumerConfig::from_env();
            cdc_consumer::run_fleet(cfg).await?;
        }
    }

    Ok(())
}
