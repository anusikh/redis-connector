//! Core models shared across the CDC connector.
//!
//! Contains the in-memory representation of PostgreSQL logical replication
//! events (`pgoutput`) independent of where they come from (producer) or
//! where they go (consumer).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A single decoded column value as found in a `pgoutput` tuple.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColData {
    Null,
    UnchangedToast,
    Text(String),
}

impl ColData {
    /// Best-effort string representation, suitable for serialization to a
    /// Redis stream field.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            ColData::Text(s) => Some(s),
            ColData::Null => None,
            ColData::UnchangedToast => None,
        }
    }
}

/// Metadata describing a single column of a replicated relation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub flags: u8,
    pub name: String,
    pub type_oid: u32,
    pub type_modifier: i32,
}

impl ColumnInfo {
    /// Whether this column is part of the replica identity / primary key.
    pub fn is_key(&self) -> bool {
        self.flags & 0x01 != 0
    }
}

/// Metadata describing a replicated table (a `Relation` message).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationInfo {
    pub namespace: String,
    pub table_name: String,
    pub replica_identity: u8,
    pub columns: Vec<ColumnInfo>,
}

/// A row-level change emitted by the logical replication stream.
///
/// Every variant carries `key`, the primary-key columns of the affected row.
/// This is used both for partition routing in the producer and for building a
/// stable OpenSearch document id in the consumer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TxnEvent {
    Insert {
        table: String,
        key: HashMap<String, String>,
        data: HashMap<String, String>,
    },
    Update {
        table: String,
        key: HashMap<String, String>,
        data: HashMap<String, String>,
    },
    Delete {
        table: String,
        key: HashMap<String, String>,
    },
}

impl TxnEvent {
    pub fn table(&self) -> &str {
        match self {
            TxnEvent::Insert { table, .. }
            | TxnEvent::Update { table, .. }
            | TxnEvent::Delete { table, .. } => table,
        }
    }

    pub fn key(&self) -> &HashMap<String, String> {
        match self {
            TxnEvent::Insert { key, .. }
            | TxnEvent::Update { key, .. }
            | TxnEvent::Delete { key, .. } => key,
        }
    }

    /// Stable, unique document id for OpenSearch, e.g.
    /// `users:id=5:email=foo@bar.com`.
    pub fn document_id(&self) -> String {
        let mut pairs: Vec<(&String, &String)> = self.key().iter().collect();
        pairs.sort_by(|a, b| a.0.cmp(b.0));
        let key_part = pairs
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(":");
        format!("{}:{}", self.table(), key_part)
    }
}

/// A fully parsed PostgreSQL transaction, produced by the WAL parser and
/// consumed (after serialization) by the Redis stream producer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedTransaction {
    pub commit_lsn: u64,
    pub events: Vec<TxnEvent>,
}

impl ParsedTransaction {
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}
