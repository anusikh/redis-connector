use bytes::Buf;
use std::collections::HashMap;

#[derive(Debug)]
enum ColData {
    Null,
    UnchangedToast,
    Text(String),
}

#[derive(Debug, Clone)]
struct ColumnInfo {
    flags: u8,
    name: String,
    type_oid: u32,
    type_modifier: i32,
}

#[derive(Debug, Clone)]
struct RelationInfo {
    namespace: String,
    table_name: String,
    replica_identity: u8,
    columns: Vec<ColumnInfo>,
}

#[derive(Debug)]
enum TxnEvent {
    Insert {
        table: String,
        data: HashMap<String, String>,
    },
    Update {
        table: String,
        old_key: HashMap<String, String>,
        data: HashMap<String, String>,
    },
    Delete {
        table: String,
        old_key: HashMap<String, String>,
    },
}

pub struct PgoutputParser {
    relation_cache: HashMap<u32, RelationInfo>,
    txn_buffer: Vec<TxnEvent>,
    current_xid: u32,
    current_final_lsn: u64,
}

impl PgoutputParser {
    pub fn new() -> Self {
        Self {
            relation_cache: HashMap::new(),
            txn_buffer: Vec::new(),
            current_xid: 0,
            current_final_lsn: 0,
        }
    }

    pub fn parse_message(&mut self, data: &[u8]) -> Option<u64> {
        let mut data = data;
        let mut commit_lsn = None;

        while data.has_remaining() {
            let msg_type = data.get_u8();
            println!("\n========================================");
            println!("Message Type: {} ({:#04x})", msg_type as char, msg_type);
            println!("========================================");

            match msg_type {
                b'B' => self.parse_begin(&mut data),
                b'R' => self.parse_relation(&mut data),
                b'I' => self.parse_insert(&mut data),
                b'U' => self.parse_update(&mut data),
                b'D' => self.parse_delete(&mut data),
                b'C' => commit_lsn = Some(self.parse_commit(&mut data)),
                _ => {
                    println!("Unknown message type byte: {}", msg_type);
                    break;
                }
            }
        }

        commit_lsn
    }

    fn parse_begin(&mut self, data: &mut &[u8]) {
        let final_lsn = data.get_u64();
        let timestamp = data.get_u64();
        let xid = data.get_u32();

        self.txn_buffer.clear();
        self.current_xid = xid;
        self.current_final_lsn = final_lsn;

        println!("BEGIN - XID: {}, Final LSN: {:X}, Timestamp: {}", xid, final_lsn, timestamp);
    }

    fn parse_relation(&mut self, data: &mut &[u8]) {
        let relation_id = data.get_u32();
        let namespace = parse_cstring(data);
        let table_name = parse_cstring(data);
        let replica_identity = data.get_u8();
        let num_columns = data.get_u16();

        println!("RELATION - ID: {}, Schema: {}, Table: {}, ReplicaIdentity: {}, NumColumns: {}",
            relation_id, namespace, table_name, replica_identity, num_columns);

        let mut columns = Vec::with_capacity(num_columns as usize);
        for _ in 0..num_columns {
            let flags: u8 = data.get_u8();
            let name = parse_cstring(data);
            let type_oid = data.get_u32();
            let type_modifier = data.get_i32();

            let is_pk = flags & 0x01 != 0;
            println!("  Column: name={}, flags={} (is_pk={}), type_oid={}, type_modifier={}",
                name, flags, is_pk, type_oid, type_modifier);

            columns.push(ColumnInfo { flags, name, type_oid, type_modifier });
        }

        self.relation_cache.insert(relation_id, RelationInfo {
            namespace,
            table_name,
            replica_identity,
            columns,
        });
    }

    fn parse_insert(&mut self, data: &mut &[u8]) {
        let relation_id = data.get_u32();
        let tuple_type = data.get_u8();
        assert_eq!(tuple_type, b'N', "Expected 'N' tuple type in Insert, got '{}'", tuple_type as char);

        let rel = self.relation_cache
            .get(&relation_id)
            .unwrap_or_else(|| panic!("Relation ID {} not found in cache", relation_id));

        let num_columns = data.get_u16();
        let row_data = parse_tuple(data, num_columns);
        let data_map = zip_columns(&rel.columns, &row_data);

        println!("INSERT - Table: {}, Relation ID: {}", rel.table_name, relation_id);
        println!("  Parsed Row Data: {:#?}", data_map);

        self.txn_buffer.push(TxnEvent::Insert {
            table: rel.table_name.clone(),
            data: data_map,
        });
    }

    fn parse_update(&mut self, data: &mut &[u8]) {
        let relation_id = data.get_u32();

        let rel = self.relation_cache
            .get(&relation_id)
            .unwrap_or_else(|| panic!("Relation ID {} not found in cache", relation_id));

        println!("UPDATE - Table: {}, Relation ID: {}", rel.table_name, relation_id);

        let mut old_key: Option<HashMap<String, String>> = None;
        let mut new_data: Option<HashMap<String, String>> = None;

        while data.has_remaining() {
            let tuple_type = data.get_u8();

            match tuple_type {
                b'K' | b'O' => {
                    let tuple_name = match tuple_type {
                        b'K' => "Key Tuple (Primary Key changed)",
                        b'O' => "Old Tuple",
                        _ => unreachable!(),
                    };
                    println!("  Parsing block: {}", tuple_name);

                    let num_columns = data.get_u16();
                    let row_data = parse_tuple(data, num_columns);
                    println!("  Parsed Row Data: {:#?}", row_data);

                    old_key = Some(extract_key_columns(&rel.columns, &row_data));
                }
                b'N' => {
                    println!("  Parsing block: New Tuple");

                    let num_columns = data.get_u16();
                    let row_data = parse_tuple(data, num_columns);
                    println!("  Parsed Row Data: {:#?}", row_data);

                    new_data = Some(zip_columns(&rel.columns, &row_data));
                }
                _ => {
                    println!("  Unexpected byte identifier in update: {}", tuple_type);
                    break;
                }
            }
        }

        let old_key = old_key.unwrap_or_default();
        let new_data = new_data.expect("Update must have an 'N' (New Tuple) block");

        self.txn_buffer.push(TxnEvent::Update {
            table: rel.table_name.clone(),
            old_key,
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

        let rel = self.relation_cache
            .get(&relation_id)
            .unwrap_or_else(|| panic!("Relation ID {} not found in cache", relation_id));

        let tuple_name = match tuple_type {
            b'K' => "Key Tuple (PK changed)",
            b'O' => "Old Tuple",
            _ => unreachable!(),
        };

        println!("DELETE - Table: {}, Relation ID: {}", rel.table_name, relation_id);
        println!("  Parsing block: {}", tuple_name);

        let num_columns = data.get_u16();
        let row_data = parse_tuple(data, num_columns);
        println!("  Parsed Row Data: {:#?}", row_data);

        let old_key = extract_key_columns(&rel.columns, &row_data);

        self.txn_buffer.push(TxnEvent::Delete {
            table: rel.table_name.clone(),
            old_key,
        });
    }

    fn parse_commit(&mut self, data: &mut &[u8]) -> u64 {
        let flags = data.get_u8();
        let commit_lsn = data.get_u64();
        let end_lsn = data.get_u64();
        let commit_timestamp = data.get_u64();

        println!("COMMIT - Flags: {}, CommitLSN: {:X}, EndLSN: {:X}, Timestamp: {}",
            flags, commit_lsn, end_lsn, commit_timestamp);
        println!("  Transaction XID: {}", self.current_xid);
        println!("  Events in buffer: {}", self.txn_buffer.len());

        for (i, event) in self.txn_buffer.iter().enumerate() {
            println!("  Event [{}]: {:#?}", i, event);
        }

        self.txn_buffer.clear();
        println!("Commit complete. Transaction buffer cleared.");

        commit_lsn
    }
}

fn parse_cstring(data: &mut &[u8]) -> String {
    let pos = data.iter().position(|&b| b == 0).expect("null terminator not found in cstring");
    let s = std::str::from_utf8(&data[..pos]).unwrap_or("<invalid utf8>").to_string();
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
    columns.iter().zip(row_data.iter())
        .filter_map(|(col, val)| match val {
            ColData::Text(s) => Some((col.name.clone(), s.clone())),
            ColData::Null => Some((col.name.clone(), "NULL".to_string())),
            ColData::UnchangedToast => None,
        })
        .collect()
}

fn extract_key_columns(columns: &[ColumnInfo], row_data: &[ColData]) -> HashMap<String, String> {
    columns.iter().zip(row_data.iter())
        .filter(|(col, _)| col.flags & 0x01 != 0)
        .filter_map(|(col, val)| match val {
            ColData::Text(s) => Some((col.name.clone(), s.clone())),
            ColData::Null => Some((col.name.clone(), "NULL".to_string())),
            ColData::UnchangedToast => None,
        })
        .collect()
}