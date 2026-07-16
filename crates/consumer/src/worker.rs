//! CDC consumer worker fleet.
//!
//! A single `cdc-consumer` process runs `N` isolated Tokio tasks, one pinned to
//! each `events:P` partition, all joined in the `os_indexer` consumer group.
//! Each task reads via `XREADGROUP`, transforms change events into OpenSearch
//! `_bulk` operations (external versioning for idempotency), sends them, and
//! `XACK`s. `XAUTOCLAIM` reclaims orphaned messages; poison pills go to a DLQ.

use crate::config::ConsumerConfig;
use crate::opensearch::{BulkItem, BulkOp, ItemOutcome, OpenSearchClient};
use cdc_core::TxnEvent;
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use redis::streams::StreamReadOptions;
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Instant;
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;

const IDLE_POLL: Duration = Duration::from_millis(200);
const BACKOFF: Duration = Duration::from_secs(1);

#[derive(Deserialize)]
struct Envelope {
    /// External version for OpenSearch (`commit_lsn` + in-txn index).
    version: u64,
    event: TxnEvent,
}

struct PartitionWorker {
    conn: ConnectionManager,
    os: OpenSearchClient,
    cfg: ConsumerConfig,
    partition: usize,
    consumer: String,
    /// Per-message retry counter for mapping (poison-pill) failures.
    attempts: HashMap<String, u8>,
}

impl PartitionWorker {
    async fn fetch(&mut self) -> redis::RedisResult<Vec<(String, String)>> {
        let stream = self.cfg.stream_key(self.partition);
        let opts = StreamReadOptions::default()
            .group(&self.cfg.group_name, &self.consumer)
            .count(self.cfg.batch_size);
        let reply: redis::Value = self.conn.xread_options(&[stream], &[">"], &opts).await?;
        Ok(parse_xreadgroup(&reply))
    }

    async fn reclaim(&mut self) -> redis::RedisResult<Vec<(String, String)>> {
        let stream = self.cfg.stream_key(self.partition);
        let reply: redis::Value = redis::cmd("XAUTOCLAIM")
            .arg(&stream)
            .arg(&self.cfg.group_name)
            .arg(&self.consumer)
            .arg(self.cfg.claim_idle_ms)
            .arg("0")
            .arg("COUNT")
            .arg(self.cfg.batch_size)
            .query_async(&mut self.conn)
            .await?;
        // XAUTOCLAIM reply: [next-cursor, entries, attempted]
        if let redis::Value::Array(parts) = reply {
            if parts.len() >= 2 {
                return Ok(parse_entries_value(&parts[1]));
            }
        }
        Ok(Vec::new())
    }

    /// Process a batch of (id, raw-payload) messages. Returns `true` if a
    /// transient (OpenSearch/Redis) failure occurred and the caller should
    /// back off before fetching again.
    async fn process_batch(&mut self, messages: Vec<(String, String)>) -> bool {
        if messages.is_empty() {
            return false;
        }

        let mut items: Vec<BulkItem> = Vec::with_capacity(messages.len());
        let mut ok_index: Vec<usize> = Vec::with_capacity(messages.len());
        let mut transient = false;

        for (id, raw) in &messages {
            let envelope: Envelope = match serde_json::from_str(raw) {
                Ok(e) => e,
                Err(_) => {
                    // Malformed payload -> dead-letter immediately.
                    self.send_to_dlq(raw).await;
                    let _: redis::RedisResult<()> = self
                        .conn
                        .xack(
                            self.cfg.stream_key(self.partition),
                            &self.cfg.group_name,
                            &[id.as_str()],
                        )
                        .await;
                    self.attempts.remove(id);
                    continue;
                }
            };

            let version = envelope.version;
            let index = self.cfg.index_for(envelope.event.table());
            let doc_id = envelope.event.document_id();
            let (op, source) = match &envelope.event {
                TxnEvent::Insert { data, .. } | TxnEvent::Update { data, .. } => (
                    BulkOp::Index,
                    Some(serde_json::to_value(data).unwrap_or(serde_json::Value::Null)),
                ),
                TxnEvent::Delete { .. } => (BulkOp::Delete, None),
            };

            items.push(BulkItem {
                op,
                index,
                doc_id,
                version,
                source,
            });
            ok_index.push(items.len() - 1);
        }

        if items.is_empty() {
            return false;
        }

        let outcomes = self.os.bulk(&items).await;
        for (slot, id_and_raw) in ok_index.into_iter().enumerate() {
            let (id, raw) = &messages[id_and_raw];
            match outcomes.get(slot) {
                Some(ItemOutcome::Ack) | Some(ItemOutcome::Stale) => {
                    self.ack(id).await;
                    self.attempts.remove(id);
                }
                Some(ItemOutcome::Poison) => {
                    let count = self.attempts.entry(id.clone()).or_insert(0);
                    *count += 1;
                    if *count >= self.cfg.poison_max_retries {
                        self.send_to_dlq(raw).await;
                        self.ack(id).await;
                        self.attempts.remove(id);
                    }
                    // else: leave unacknowledged for a later reclaim/retry.
                }
                Some(ItemOutcome::Transient) | None => {
                    transient = true; // leave unacknowledged.
                }
            }
        }

        transient
    }

    async fn ack(&mut self, id: &str) {
        let _: redis::RedisResult<()> = self
            .conn
            .xack(
                self.cfg.stream_key(self.partition),
                &self.cfg.group_name,
                &[id],
            )
            .await;
    }

    async fn send_to_dlq(&mut self, raw: &str) {
        let _: redis::RedisResult<String> = self
            .conn
            .xadd(&self.cfg.dlq_stream, "*", &[("payload", raw)])
            .await;
    }

    async fn run(mut self, token: CancellationToken) {
        let mut last_claim = Instant::now();
        loop {
            if token.is_cancelled() {
                break;
            }

            let now = Instant::now();
            if now.duration_since(last_claim) >= self.cfg.claim_interval {
                if let Ok(claimed) = self.reclaim().await {
                    self.process_batch(claimed).await;
                }
                last_claim = Instant::now();
            }

            match self.fetch().await {
                Ok(messages) => {
                    let transient = self.process_batch(messages).await;
                    if transient {
                        sleep(BACKOFF).await;
                    } else {
                        sleep(IDLE_POLL).await;
                    }
                }
                Err(e) => {
                    eprintln!("partition {} fetch error: {e} (retrying)", self.partition);
                    if !token.is_cancelled() {
                        sleep(BACKOFF).await;
                    }
                }
            }
        }
        println!("partition {} shutting down", self.partition);
    }
}

/// Run the full worker fleet: ensure groups, spawn N partition tasks, wait for
/// shutdown signal, then drain and exit.
pub async fn run_fleet(cfg: ConsumerConfig) -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = redis::Client::open(cfg.redis_url.as_str())?
        .get_connection_manager()
        .await?;

    // Ensure the consumer group exists on every partition stream.
    for p in 0..cfg.partitions {
        let stream = cfg.stream_key(p);
        let _: redis::Value = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&stream)
            .arg(&cfg.group_name)
            .arg("$")
            .arg("MKSTREAM")
            .query_async(&mut conn)
            .await
            .or_else(|e| {
                if e.to_string().contains("BUSYGROUP") {
                    Ok(redis::Value::Nil)
                } else {
                    Err(e)
                }
            })?;
    }

    let os = OpenSearchClient::new(
        &cfg.opensearch_url,
        cfg.opensearch_user.clone(),
        cfg.opensearch_password.clone(),
    );

    let token = CancellationToken::new();
    let mut handles = Vec::with_capacity(cfg.partitions);
    for p in 0..cfg.partitions {
        let worker = PartitionWorker {
            conn: redis::Client::open(cfg.redis_url.as_str())?
                .get_connection_manager()
                .await?,
            os: os.clone(),
            cfg: clone_cfg(&cfg),
            partition: p,
            consumer: format!("worker-{p}"),
            attempts: HashMap::new(),
        };
        let token = token.clone();
        handles.push(tokio::spawn(async move { worker.run(token).await }));
    }

    println!(
        "Consumer fleet started: {} partitions, group `{}`, streams `{}{}..{}`",
        cfg.partitions,
        cfg.group_name,
        cfg.stream_prefix,
        0,
        cfg.partitions - 1
    );

    spawn_shutdown_watcher(token.clone());
    token.cancelled().await;

    for h in handles {
        let _ = h.await;
    }
    println!("Consumer fleet stopped");
    Ok(())
}

fn clone_cfg(cfg: &ConsumerConfig) -> ConsumerConfig {
    ConsumerConfig {
        redis_url: cfg.redis_url.clone(),
        partitions: cfg.partitions,
        group_name: cfg.group_name.clone(),
        stream_prefix: cfg.stream_prefix.clone(),
        dlq_stream: cfg.dlq_stream.clone(),
        opensearch_url: cfg.opensearch_url.clone(),
        opensearch_user: cfg.opensearch_user.clone(),
        opensearch_password: cfg.opensearch_password.clone(),
        index_prefix: cfg.index_prefix.clone(),
        batch_size: cfg.batch_size,
        claim_idle_ms: cfg.claim_idle_ms,
        claim_interval: cfg.claim_interval,
        poison_max_retries: cfg.poison_max_retries,
    }
}

#[cfg(unix)]
fn spawn_shutdown_watcher(token: CancellationToken) {
    tokio::spawn(async move {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = signal(SignalKind::terminate()).expect("sigterm");
        let mut sigint = signal(SignalKind::interrupt()).expect("sigint");
        tokio::select! {
            _ = sigterm.recv() => {}
            _ = sigint.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
        println!("shutdown signal received");
        token.cancel();
    });
}

#[cfg(not(unix))]
fn spawn_shutdown_watcher(token: CancellationToken) {
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        println!("shutdown signal received");
        token.cancel();
    });
}

/// Parse the full `XREADGROUP` reply (array of [stream, [[id, fields], ...]]).
fn parse_xreadgroup(value: &redis::Value) -> Vec<(String, String)> {
    let mut out = Vec::new();
    if let redis::Value::Array(streams) = value {
        for stream in streams {
            if let redis::Value::Array(pair) = stream {
                if pair.len() == 2 {
                    if let redis::Value::Array(_) = &pair[1] {
                        out.extend(parse_entries_value(&pair[1]));
                    }
                }
            }
        }
    }
    out
}

/// Parse an entries array value `[[id, [field, value, ...]], ...]`.
fn parse_entries_value(entries: &redis::Value) -> Vec<(String, String)> {
    let mut out = Vec::new();
    if let redis::Value::Array(entries) = entries {
        for entry in entries {
            if let redis::Value::Array(kv) = entry {
                if kv.len() == 2 {
                    if let (redis::Value::BulkString(id), redis::Value::Array(fields)) =
                        (&kv[0], &kv[1])
                    {
                        if let Some(payload) = field_value(fields, "payload") {
                            out.push((String::from_utf8_lossy(id).to_string(), payload));
                        }
                    }
                }
            }
        }
    }
    out
}

fn field_value(fields: &[redis::Value], name: &str) -> Option<String> {
    let mut iter = fields.iter();
    while let (Some(k), Some(v)) = (iter.next(), iter.next()) {
        if let (redis::Value::BulkString(kb), redis::Value::BulkString(vb)) = (k, v) {
            if String::from_utf8_lossy(kb) == name {
                return Some(String::from_utf8_lossy(vb).to_string());
            }
        }
    }
    None
}
