//! CDC producer: reads PostgreSQL logical replication (WAL), parses it, and
//! publishes the changes to Redis streams.

pub mod redis_producer;
pub mod wal;

use crate::redis_producer::RedisStreamProducer;
use crate::wal::PgoutputParser;
use pgwire_replication::{
    Lsn, ReplicationClient, ReplicationConfig, TlsConfig, client::ReplicationEvent,
};

pub struct ProducerConfig {
    pub pg_host: String,
    pub pg_port: u16,
    pub pg_user: String,
    pub pg_password: String,
    pub pg_database: String,
    pub pg_slot: String,
    pub pg_publication: String,
    pub start_lsn: String,
    pub redis_url: String,
    pub redis_stream_prefix: String,
    pub partitions: usize,
    pub producer_name: String,
}

impl ProducerConfig {
    /// Build configuration from environment variables, falling back to defaults.
    pub fn from_env() -> Self {
        let env = |name: &str, default: &str| std::env::var(name).unwrap_or_else(|_| default.to_string());
        Self {
            pg_host: env("PGHOST", "127.0.0.1"),
            pg_port: env("PGPORT", "5432").parse().unwrap_or(5432),
            pg_user: env("PGUSER", "postgres"),
            pg_password: env("PGPASSWORD", "password"),
            pg_database: env("PGDATABASE", "cdc_db"),
            pg_slot: env("PGSLOT", "my_slot"),
            pg_publication: env("PGPUBLICATION", "users_pub"),
            start_lsn: env("START_LSN", "0/0"),
            redis_url: env("REDIS_URL", "redis://127.0.0.1:6379"),
            redis_stream_prefix: env("REDIS_STREAM_PREFIX", "events:"),
            partitions: env("PARTITIONS", "16").parse().unwrap_or(16),
            producer_name: env("PRODUCER_NAME", "cdc-producer-1"),
        }
    }
}

pub async fn run(cfg: ProducerConfig) -> Result<(), Box<dyn std::error::Error>> {
    let start_lsn = Lsn::parse(&cfg.start_lsn).unwrap();

    let repl_cfg = ReplicationConfig {
        host: cfg.pg_host.clone(),
        port: cfg.pg_port,
        user: cfg.pg_user.clone(),
        password: cfg.pg_password.clone(),
        database: cfg.pg_database.clone(),
        tls: TlsConfig::disabled(),
        slot: cfg.pg_slot.clone(),
        publication: cfg.pg_publication.clone(),
        start_lsn,
        stop_at_lsn: None,
        status_interval: std::time::Duration::from_secs(1),
        idle_wakeup_interval: std::time::Duration::from_secs(30),
        buffer_events: 8192,
    };

    let mut producer = RedisStreamProducer::connect(
        &cfg.redis_url,
        cfg.redis_stream_prefix.clone(),
        cfg.partitions,
        cfg.producer_name.clone(),
    )
    .await?;
    println!("Connected to Redis at {}", cfg.redis_url);

    let mut repl = ReplicationClient::connect(repl_cfg).await?;
    let mut parser = PgoutputParser::new();
    println!("Connected to PostgreSQL replication slot {}", cfg.pg_slot);

    loop {
        match repl.recv().await {
            Ok(Some(ReplicationEvent::Begin { final_lsn, xid, .. })) => {
                parser.begin(xid, final_lsn.into());
            }
            Ok(Some(ReplicationEvent::XLogData { data, .. })) => {
                parser.parse_frame(&data);
            }
            Ok(Some(ReplicationEvent::Commit { lsn, .. })) => {
                let txn = parser.commit(lsn.into());
                println!(
                    "COMMIT lsn={:X} events={} -> publishing to Redis",
                    txn.commit_lsn,
                    txn.events.len()
                );
                // Critical Rule: only acknowledge the LSN to Postgres AFTER a
                // successful XADD to Redis.
                producer.publish(&txn).await?;
                repl.update_applied_lsn(lsn.into());
            }
            Ok(Some(ReplicationEvent::KeepAlive { .. })) => {
                // `pgwire` handles keepalive replies internally; nothing to do.
            }
            Ok(Some(ReplicationEvent::StoppedAt { reached })) => {
                println!("Replication stopped at {reached}");
                break;
            }
            Ok(Some(ReplicationEvent::Message { .. })) => {}
            Ok(None) => {
                println!("Replication ended cleanly");
                break;
            }
            Err(e) => {
                eprintln!("Replication failed: {e}");
                return Err(e.into());
            }
        }
    }

    Ok(())
}
