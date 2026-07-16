//! Minimal OpenSearch `_bulk` client over reqwest.
//!
//! Builds the NDJSON bulk request manually so we have full control over
//! `version_type=external` and per-item status classification (needed for the
//! idempotency / poison-pill resilience rules).

use serde_json::{Value, json};

pub enum BulkOp {
    Index,
    Delete,
}

pub struct BulkItem {
    pub op: BulkOp,
    pub index: String,
    pub doc_id: String,
    pub version: u64,
    /// Document body for index operations; `None` for deletes.
    pub source: Option<Value>,
}

/// Outcome for a single bulk item, classified from the response.
#[derive(Clone)]
pub enum ItemOutcome {
    /// Successfully indexed/deleted.
    Ack,
    /// Version conflict (409): stale/out-of-order, safe to acknowledge.
    Stale,
    /// Mapping / parse failure (400): counts toward the poison-pill limit.
    Poison,
    /// Cluster/network/5xx failure: leave unacknowledged and retry later.
    Transient,
}

#[derive(Clone)]
pub struct OpenSearchClient {
    http: reqwest::Client,
    base: String,
}

impl OpenSearchClient {
    pub fn new(url: &str, user: Option<String>, password: Option<String>) -> Self {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            reqwest::header::HeaderValue::from_static("application/x-ndjson"),
        );
        if let (Some(u), Some(p)) = (user, password) {
            let creds = format!("{}:{}", u, p);
            let encoded = base64_encode(creds.as_bytes());
            if let Ok(val) = reqwest::header::HeaderValue::from_str(&format!("Basic {}", encoded)) {
                headers.insert(reqwest::header::AUTHORIZATION, val);
            }
        }
        let http = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .expect("failed to build OpenSearch http client");
        Self {
            http,
            base: url.trim_end_matches('/').to_string(),
        }
    }

    /// Send a bulk request. Returns one outcome per input item, in order.
    pub async fn bulk(&self, items: &[BulkItem]) -> Vec<ItemOutcome> {
        if items.is_empty() {
            return Vec::new();
        }

        let mut body = String::new();
        for it in items {
            let action = match it.op {
                BulkOp::Index => json!({
                    "index": {
                        "_index": it.index,
                        "_id": it.doc_id,
                        "version": it.version,
                        "version_type": "external",
                    }
                }),
                BulkOp::Delete => json!({
                    "delete": {
                        "_index": it.index,
                        "_id": it.doc_id,
                        "version": it.version,
                        "version_type": "external",
                    }
                }),
            };
            body.push_str(&action.to_string());
            body.push('\n');
            if let Some(src) = &it.source {
                body.push_str(&src.to_string());
                body.push('\n');
            }
        }

        let res = self
            .http
            .post(format!("{}/_bulk", self.base))
            .body(body)
            .send()
            .await;

        let res = match res {
            Ok(r) => r,
            Err(_) => return vec![ItemOutcome::Transient; items.len()],
        };

        if !res.status().is_success() {
            return vec![ItemOutcome::Transient; items.len()];
        }

        let v: Value = match res.json().await {
            Ok(v) => v,
            Err(_) => return vec![ItemOutcome::Transient; items.len()],
        };

        classify_response(&v, items.len())
    }
}

fn classify_response(resp: &Value, n: usize) -> Vec<ItemOutcome> {
    let mut out = Vec::with_capacity(n);
    if let Some(items) = resp.get("items").and_then(|i| i.as_array()) {
        for item in items {
            // Each item is {"index": {...}} or {"delete": {...}}.
            let obj = item
                .as_object()
                .and_then(|m| m.values().next())
                .and_then(|v| v.as_object());
            let (status, err_type) = match obj {
                Some(o) => (
                    o.get("status").and_then(|s| s.as_u64()).unwrap_or(0),
                    o.get("error")
                        .and_then(|e| e.get("type"))
                        .and_then(|t| t.as_str())
                        .unwrap_or(""),
                ),
                None => (0, ""),
            };
            out.push(classify(status, err_type));
        }
    }
    // Pad if the response item count doesn't match (defensive).
    while out.len() < n {
        out.push(ItemOutcome::Transient);
    }
    out
}

fn classify(status: u64, err_type: &str) -> ItemOutcome {
    match status {
        200 | 201 => ItemOutcome::Ack,
        409 => ItemOutcome::Stale,
        400 if err_type.contains("mapper_parsing") || err_type.contains("parsing_exception") => {
            ItemOutcome::Poison
        }
        400 => ItemOutcome::Poison,
        429 => ItemOutcome::Transient,
        500..=599 => ItemOutcome::Transient,
        _ => ItemOutcome::Transient,
    }
}

fn base64_encode(input: &[u8]) -> String {
    const CHARS: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in input.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | (b[2] as u32);
        out.push(CHARS[((n >> 18) & 63) as usize] as char);
        out.push(CHARS[((n >> 12) & 63) as usize] as char);
        if chunk.len() > 1 {
            out.push(CHARS[((n >> 6) & 63) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(CHARS[(n & 63) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}
