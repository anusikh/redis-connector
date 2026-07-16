//! Consumer configuration, built from environment variables.

use std::time::Duration;

pub struct ConsumerConfig {
    pub redis_url: String,
    /// Number of partitions. Must match the producer's `PARTITIONS`.
    pub partitions: usize,
    /// Consumer group name spanning all partitioned streams.
    pub group_name: String,
    /// Prefix for the partitioned stream keys, e.g. `events:` -> `events:0`.
    pub stream_prefix: String,
    /// Stream key used for the dead-letter queue.
    pub dlq_stream: String,
    /// OpenSearch base URL.
    pub opensearch_url: String,
    /// Optional HTTP basic-auth credentials for OpenSearch.
    pub opensearch_user: Option<String>,
    pub opensearch_password: Option<String>,
    /// Index name prefix, e.g. `cdc-` -> `cdc-users`.
    pub index_prefix: String,
    /// Max messages fetched per `XREADGROUP` call.
    pub batch_size: usize,
    /// Idle threshold (ms) after which `XAUTOCLAIM` reclaims a message.
    pub claim_idle_ms: u64,
    /// Interval between `XAUTOCLAIM` sweeps.
    pub claim_interval: Duration,
    /// Mapping failures tolerated before a message is sent to the DLQ.
    pub poison_max_retries: u8,
}

impl ConsumerConfig {
    pub fn from_env() -> Self {
        let env = |name: &str, default: &str| std::env::var(name).unwrap_or_else(|_| default.to_string());
        let user = std::env::var("OPENSEARCH_USER").ok().filter(|s| !s.is_empty());
        let password = std::env::var("OPENSEARCH_PASSWORD").ok().filter(|s| !s.is_empty());
        Self {
            redis_url: env("REDIS_URL", "redis://127.0.0.1:6379"),
            partitions: env("PARTITIONS", "16").parse().unwrap_or(16),
            group_name: env("CONSUMER_GROUP", "os_indexer"),
            stream_prefix: env("CONSUMER_STREAM_PREFIX", "events:"),
            dlq_stream: env("CONSUMER_DLQ", "events:dlq"),
            opensearch_url: env("OPENSEARCH_URL", "http://127.0.0.1:9200"),
            opensearch_user: user,
            opensearch_password: password,
            index_prefix: env("OS_INDEX_PREFIX", "cdc-"),
            batch_size: env("CONSUMER_BATCH", "500").parse().unwrap_or(500),
            claim_idle_ms: env("CLAIM_IDLE_MS", "30000").parse().unwrap_or(30000),
            claim_interval: Duration::from_secs(
                env("CLAIM_INTERVAL_S", "60").parse().unwrap_or(60),
            ),
            poison_max_retries: env("POISON_MAX_RETRIES", "3").parse().unwrap_or(3),
        }
    }

    pub fn stream_key(&self, partition: usize) -> String {
        format!("{}{}", self.stream_prefix, partition)
    }

    pub fn index_for(&self, table: &str) -> String {
        format!("{}{}", self.index_prefix, table)
    }
}
