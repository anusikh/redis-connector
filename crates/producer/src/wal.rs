//! PostgreSQL WAL (`pgoutput`) parsing.
//!
//! Turns raw logical-replication byte frames into [`cdc_core::ParsedTransaction`]
//! values that the Redis producer can emit.

use bytes::Buf;
use cdc_core::{ColData, ColumnInfo, ParsedTransaction, RelationInfo, TxnEvent};
use std::collections::HashMap;

pub struct PgoutputParser {
    relation_cache: HashMap<u32, RelationInfo>,
    txn_buffer: Vec<TxnEvent>,
}

impl PgoutputParser {
    pub fn new() -> Self {
        Self {
            relation_cache: HashMap::new(),
            txn_buffer: Vec::new(),
        }
    }

    /// Start a new transaction. `pgwire` already surfaces `Begin` as a
    /// separate `ReplicationEvent`, so we just (re)initialize the buffer here.
    pub fn begin(&mut self, _xid: u32, _final_lsn: u64) {
        self.txn_buffer.clear();
    }

    /// Parse one `XLogData` frame, which contains the row-level messages
    /// (`Relation`/`Insert`/`Update`/`Delete`). The `Begin`/`Commit` markers
    /// are delivered as separate `ReplicationEvent`s by `pgwire`, not inside
    /// the frame bytes.
    pub fn parse_frame(&mut self, data: &[u8]) {
        let mut data = data;
        while data.has_remaining() {
            let msg_type = data.get_u8();
            match msg_type {
                b'R' => self.parse_relation(&mut data),
                b'I' => self.parse_insert(&mut data),
                b'U' => self.parse_update(&mut data),
                b'D' => self.parse_delete(&mut data),
                _ => break,
            }
        }
    }

    /// Finalize the current transaction on a `Commit` event.
    pub fn commit(&mut self, commit_lsn: u64) -> ParsedTransaction {
        let events = std::mem::take(&mut self.txn_buffer);
        ParsedTransaction { commit_lsn, events }
    }
    fn parse_relation(&mut self, data: &mut &[u8]) {
        let relation_id = data.get_u32();
        let namespace = parse_cstring(data);
        let table_name = parse_cstring(data);
        let replica_identity = data.get_u8();
        let num_columns = data.get_u16();

        let mut columns = Vec::with_capacity(num_columns as usize);
        for _ in 0..num_columns {
            let flags: u8 = data.get_u8();
            let name = parse_cstring(data);
            let type_oid = data.get_u32();
            let type_modifier = data.get_i32();

            columns.push(ColumnInfo {
                flags,
                name,
                type_oid,
                type_modifier,
            });
        }

        self.relation_cache.insert(
            relation_id,
            RelationInfo {
                namespace,
                table_name,
                replica_identity,
                columns,
            },
        );
    }

    fn parse_insert(&mut self, data: &mut &[u8]) {
        let relation_id = data.get_u32();
        let tuple_type = data.get_u8();
        assert_eq!(
            tuple_type, b'N',
            "Expected 'N' tuple type in Insert, got '{}'",
            tuple_type as char
        );

        let rel = self
            .relation_cache
            .get(&relation_id)
            .unwrap_or_else(|| panic!("Relation ID {} not found in cache", relation_id));

        let num_columns = data.get_u16();
        let row_data = parse_tuple(data, num_columns);
        let key = extract_key_columns(&rel.columns, &row_data);
        let data_map = zip_columns(&rel.columns, &row_data);

        self.txn_buffer.push(TxnEvent::Insert {
            table: rel.table_name.clone(),
            key,
            data: data_map,
        });
    }

    fn parse_update(&mut self, data: &mut &[u8]) {
        let relation_id = data.get_u32();
        let rel = self
            .relation_cache
            .get(&relation_id)
            .unwrap_or_else(|| panic!("Relation ID {} not found in cache", relation_id));

        let mut old_key: Option<HashMap<String, String>> = None;
        let mut new_data: Option<HashMap<String, String>> = None;
        let mut new_row: Option<Vec<ColData>> = None;

        while data.has_remaining() {
            let tuple_type = data.get_u8();

            match tuple_type {
                b'K' | b'O' => {
                    let num_columns = data.get_u16();
                    let row_data = parse_tuple(data, num_columns);
                    old_key = Some(extract_key_columns(&rel.columns, &row_data));
                }
                b'N' => {
                    let num_columns = data.get_u16();
                    let row_data = parse_tuple(data, num_columns);
                    new_row = Some(row_data.clone());
                    new_data = Some(zip_columns(&rel.columns, &row_data));
                }
                _ => break,
            }
        }

        let new_data = new_data.expect("Update must have an 'N' (New Tuple) block");

        // Prefer the old-key tuple when present. If pgoutput omitted it (it only
        // sends the new row), derive the key from the new tuple — the primary
        // key is unchanged by a normal UPDATE.
        let key = old_key
            .or_else(|| new_row.map(|rd| extract_key_columns(&rel.columns, &rd)))
            .unwrap_or_default();

        self.txn_buffer.push(TxnEvent::Update {
            table: rel.table_name.clone(),
            key,
            data: new_data,
        });
    }

    fn parse_delete(&mut self, data: &mut &[u8]) {
        let relation_id = data.get_u32();
        let tuple_type = data.get_u8();
        assert!(
            tuple_type == b'O' || tuple_type == b'K',
            "Expected 'O' or 'K' tuple type in Delete, got '{}'",
            tuple_type as char
        );

        let rel = self
            .relation_cache
            .get(&relation_id)
            .unwrap_or_else(|| panic!("Relation ID {} not found in cache", relation_id));

        let num_columns = data.get_u16();
        let row_data = parse_tuple(data, num_columns);
        let old_key = extract_key_columns(&rel.columns, &row_data);

        self.txn_buffer.push(TxnEvent::Delete {
            table: rel.table_name.clone(),
            key: old_key,
        });
    }
}

fn parse_cstring(data: &mut &[u8]) -> String {
    let pos = data
        .iter()
        .position(|&b| b == 0)
        .expect("null terminator not found in cstring");
    let s = std::str::from_utf8(&data[..pos])
        .unwrap_or("<invalid utf8>")
        .to_string();
    data.advance(pos + 1);
    s
}

fn parse_tuple(data: &mut &[u8], num_columns: u16) -> Vec<ColData> {
    let mut row_data: Vec<ColData> = Vec::with_capacity(num_columns as usize);

    for _ in 0..num_columns {
        let col_format = data.get_u8();
        match col_format {
            b'n' => row_data.push(ColData::Null),
            b'u' => row_data.push(ColData::UnchangedToast),
            b't' => {
                let len = data.get_u32() as usize;
                let col_bytes = &data[..len];
                let col_str = std::str::from_utf8(col_bytes)
                    .unwrap_or("<invalid utf8>")
                    .to_string();
                row_data.push(ColData::Text(col_str));
                data.advance(len);
            }
            _ => panic!("Unknown column format byte: {}", col_format),
        }
    }

    row_data
}

fn zip_columns(columns: &[ColumnInfo], row_data: &[ColData]) -> HashMap<String, String> {
    columns
        .iter()
        .zip(row_data.iter())
        .filter_map(|(col, val)| match val {
            ColData::Text(s) => Some((col.name.clone(), s.clone())),
            ColData::Null => Some((col.name.clone(), "NULL".to_string())),
            ColData::UnchangedToast => None,
        })
        .collect()
}

fn extract_key_columns(columns: &[ColumnInfo], row_data: &[ColData]) -> HashMap<String, String> {
    columns
        .iter()
        .zip(row_data.iter())
        .filter(|(col, _)| col.is_key())
        .filter_map(|(col, val)| match val {
            ColData::Text(s) => Some((col.name.clone(), s.clone())),
            ColData::Null => Some((col.name.clone(), "NULL".to_string())),
            ColData::UnchangedToast => None,
        })
        .collect()
}
