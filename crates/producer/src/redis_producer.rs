//! Redis Stream producer: emits parsed CDC transactions into partitioned
//! Redis streams.

use cdc_core::{ParsedTransaction, TxnEvent};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use serde_json::json;

pub struct RedisStreamProducer {
    conn: ConnectionManager,
    /// Prefix applied to every stream key, e.g. `events:` -> `events:0`.
    stream_prefix: String,
    /// Number of partitions. A row is routed to `hash(table+key) % partitions`.
    partitions: usize,
    /// Logical name attached to every event (useful for debugging).
    producer_name: String,
}

impl RedisStreamProducer {
    pub async fn connect(
        url: &str,
        stream_prefix: String,
        partitions: usize,
        producer_name: String,
    ) -> redis::RedisResult<Self> {
        let client = redis::Client::open(url)?;
        let conn = client.get_connection_manager().await?;
        Ok(Self {
            conn,
            stream_prefix,
            partitions: partitions.max(1),
            producer_name,
        })
    }

    /// Partition for an event: stable FNV-1a over the document id, mod N.
    /// Stable across restarts (unlike `DefaultHasher`) so a row always lands on
    /// the same stream, preserving per-row ordering.
    fn partition_of(&self, event: &TxnEvent) -> usize {
        fnv1a_64(event.document_id().as_bytes()) as usize % self.partitions
    }

    fn stream_key(&self, partition: usize) -> String {
        format!("{}{}", self.stream_prefix, partition)
    }

    /// Publish a single transaction. Each row-level event becomes one entry in
    /// the stream for its partition.
    pub async fn publish(&mut self, txn: &ParsedTransaction) -> redis::RedisResult<()> {
        for (i, event) in txn.events.iter().enumerate() {
            let op = match event {
                TxnEvent::Insert { .. } => "insert",
                TxnEvent::Update { .. } => "update",
                TxnEvent::Delete { .. } => "delete",
            };

            // Version = commit_lsn + in-transaction index. Multiple changes to
            // the same row inside one transaction share a commit_lsn; the index
            // keeps their external versions strictly increasing so OpenSearch
            // applies them in order instead of rejecting later ones as stale.
            let version = txn.commit_lsn + i as u64;

            let payload = json!({
                "producer": self.producer_name,
                "commit_lsn": format!("{:X}", txn.commit_lsn),
                "version": version,
                "op": op,
                "event": event,
            });

            let partition = self.partition_of(event);
            let key = self.stream_key(partition);
            let _id: String = self
                .conn
                .xadd(&key, "*", &[("payload", payload.to_string())])
                .await?;
        }
        Ok(())
    }
}

/// FNV-1a 64-bit hash. Deterministic and dependency-free.
fn fnv1a_64(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    let mut hash = OFFSET;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}
