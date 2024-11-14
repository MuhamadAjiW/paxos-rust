use std::{
    collections::HashMap,
    io::{self},
    time::Duration,
};

use bincode::{deserialize, serialize};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    time::timeout,
};

use crate::{
    base_libs::operation::{BinKV, Operation, OperationType},
    network::{receive_message, send_message},
    types::PaxosMessage,
};

use super::node::Node;

pub struct Store {
    map: HashMap<String, String>,
    wal_path: String,
    // sstables: Vec<String>,
}

impl Store {
    pub fn new() -> Self {
        return Store {
            map: HashMap::new(),
            wal_path: "./wal.ignore".to_string(),
        };
    }

    pub fn set(&mut self, key: &str, value: &str) {
        self.map.insert(key.to_string(), value.to_string());
    }

    pub fn get(&self, key: &str) -> String {
        return self.map.get(key).cloned().unwrap_or("".to_string());
    }

    pub fn remove(&mut self, key: &str) -> String {
        return self.map.remove(key).unwrap_or("".to_string());
    }

    // _TODO: Persistent log storage
    pub fn process_request(&mut self, request: &Operation) -> String {
        let response;

        match request.op_type {
            OperationType::BAD => {
                response = format!("Bad command received\n");
            }
            OperationType::PING => {
                response = format!("PONG\n");
            }
            OperationType::GET => {
                let result = self.get(&request.kv.key);

                if self.get(&request.kv.key).is_empty() {
                    response = String::new();
                } else {
                    response = format!("{}\n", result);
                }
            }
            OperationType::SET => {
                self.set(
                    &request.kv.key.clone(),
                    String::from_utf8(request.kv.value.clone())
                        .unwrap()
                        .as_str(),
                );
                response = "OK\n".to_string();
            }
            OperationType::DELETE => {
                self.remove(&request.kv.key);
                response = "OK\n".to_string();
            }
        }

        response
    }

    // _TODO: Persistent data inside files
    pub async fn persist_request_ec(&mut self, operation_ec: &Operation) -> Result<(), io::Error> {
        match operation_ec.op_type {
            OperationType::SET | OperationType::DELETE => {
                self.write_to_wal(operation_ec).await?;
            }
            _ => {}
        }

        Ok(())
    }

    // fn flush_to_sstable(kv: &BinKV, file: &mut std::fs::File) {
    //     let encoded = bincode::serialize(&kv).unwrap();
    //     file.write_all(&encoded).unwrap();
    // }

    pub async fn get_from_wal(&self, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        let wal_file = File::open(&self.wal_path).await?;
        let mut reader = BufReader::new(wal_file);
        let mut buffer = Vec::new();

        let mut value: Option<Vec<u8>> = None;

        while reader.read_to_end(&mut buffer).await? > 0 {
            let operation = &buffer[..3];
            let kv_data = &buffer[3..];

            let kv: BinKV = deserialize(kv_data).unwrap();

            if kv.key == key {
                if operation == b"SET" {
                    value = Some(kv.value);
                } else if operation == b"REM" {
                    value = None;
                }
            }
        }

        Ok(value)
    }

    pub async fn write_to_wal(&self, operation_ec: &Operation) -> Result<(), io::Error> {
        let mut wal = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.wal_path)
            .await?;

        wal.write_all(operation_ec.op_type.to_string().as_bytes())
            .await?;

        let encoded = serialize(&operation_ec.kv).unwrap();
        wal.write_all(&encoded).await?;
        wal.flush().await?;

        Ok(())
    }
}

impl Node {
    pub async fn get_from_cluster(
        &self,
        key: &String,
    ) -> Result<String, reed_solomon_erasure::Error> {
        let mut result: String = String::new();
        match self.store.get_from_wal(key).await {
            Ok(Some(value)) => {
                let follower_list: Vec<String> = {
                    let followers_guard = self.cluster_list.lock().unwrap();
                    followers_guard.iter().cloned().collect()
                };

                // _TODO: base on index
                let mut recovery: Vec<Option<Vec<u8>>> = vec![Some(value)];
                let mut remote_shards: Vec<Option<Vec<u8>>> =
                    self.broadcast_get_value(&follower_list, key).await;
                recovery.append(&mut remote_shards);

                self.ec.reconstruct(&mut recovery)?;

                result = recovery
                    .iter()
                    .filter_map(|opt| opt.as_ref().map(|v| String::from_utf8(v.clone()).unwrap()))
                    .collect::<Vec<String>>()
                    .join("");
            }
            Ok(None) => {
                println!("No value found");
            }
            Err(e) => {
                eprintln!("Error while reading from WAL: {}", e);
            }
        }

        Ok(result)
    }

    // TODO: Implement
    pub async fn handle_recovery_request(&self, _key: &str) {}

    // TODO: Implement
    async fn broadcast_get_value(
        &self,
        follower_list: &Vec<String>,
        key: &str,
    ) -> Vec<Option<Vec<u8>>> {
        // let mut acks = 0;

        for follower_addr in follower_list {
            // Send the request to the follower
            send_message(
                &self.socket,
                PaxosMessage::RecoveryRequest {
                    key: key.to_string(),
                },
                follower_addr,
            )
            .await
            .unwrap();
            println!(
                "Leader broadcasted request to follower at {}",
                follower_addr
            );

            // TODO: Should not be blocking, use state management inside the class instead
            // Wait for acknowledgment with timeout (ex. 2 seconds)
            match timeout(Duration::from_secs(2), receive_message(&self.socket)).await {
                Ok(Ok((ack, _))) => {
                    if let PaxosMessage::RecoveryReply { .. } = ack {
                        // acks += 1;
                        println!(
                            "Leader received acknowledgment from follower at {}",
                            follower_addr
                        );
                    }
                }
                Ok(Err(e)) => {
                    println!(
                        "Error receiving acknowledgment from follower at {}: {}",
                        follower_addr, e
                    );
                }
                Err(_) => {
                    println!(
                        "Timeout waiting for acknowledgment from follower at {}",
                        follower_addr
                    );
                }
            }
        }
        vec![]
    }
}
