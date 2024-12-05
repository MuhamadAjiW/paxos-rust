use std::{
    collections::HashMap,
    io::{self},
    sync::Arc,
    time::Duration,
    vec,
};

use bincode::{deserialize, serialize};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    task::JoinSet,
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
    pub fn new(wal_path: &String) -> Self {
        return Store {
            map: HashMap::new(),
            wal_path: wal_path.clone(),
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

    // _TODO: Fetching data from files
    pub async fn get_from_wal(&self, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        let wal_file = File::open(&self.wal_path).await?;
        let mut reader = BufReader::new(wal_file);
        let mut value: Option<Vec<u8>> = None;

        loop {
            let mut op_buf = [0; 3];
            if reader.read_exact(&mut op_buf).await.is_err() {
                break;
            }

            let mut key_len_buf = [0; 8];
            reader.read_exact(&mut key_len_buf).await?;
            let key_len = usize::from_le_bytes(key_len_buf);

            let mut key_buf = vec![0; key_len];
            reader.read_exact(&mut key_buf).await?;

            let mut value_len_buf = [0; 8];
            reader.read_exact(&mut value_len_buf).await?;
            let value_len = usize::from_le_bytes(value_len_buf);

            let mut value_buf = vec![0; value_len];
            reader.read_exact(&mut value_buf).await?;

            let mut kv_buf: Vec<u8> = Vec::with_capacity(8 + key_len + 8 + value_len);
            kv_buf.extend(&key_len_buf);
            kv_buf.extend(&key_buf);
            kv_buf.extend(&value_len_buf);
            kv_buf.extend(&value_buf);

            println!("kv_buf: {:?}", kv_buf);
            let kv: BinKV = deserialize(&kv_buf).unwrap();

            if kv.key == key {
                if &op_buf == b"SET" {
                    value = Some(kv.value);
                } else if &op_buf == b"DEL" {
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
        println!("serialized data: {:?}", encoded);
        wal.write_all(&encoded).await?;
        wal.flush().await?;

        Ok(())
    }
}

impl Node {
    pub async fn get_from_cluster(
        &mut self,
        key: &String,
    ) -> Result<String, reed_solomon_erasure::Error> {
        let mut result: String = String::new();
        match self.store.get_from_wal(key).await {
            Ok(Some(value)) => {
                let follower_list: Vec<String> = {
                    let followers_guard = self.cluster_list.lock().unwrap();
                    followers_guard.iter().cloned().collect()
                };

                let mut recovery: Vec<Option<Vec<u8>>> = self
                    .broadcast_get_value(&follower_list, &Some(value), key)
                    .await;

                self.ec.reconstruct(&mut recovery)?;

                result = recovery
                    .iter()
                    .take(self.ec.shard_count)
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

        self.store.set(key, &result);

        Ok(result)
    }

    pub async fn handle_recovery_request(&self, src_addr: &String, key: &str) {
        match self.store.get_from_wal(key).await {
            Ok(Some(value)) => {
                // Send the data to the requestor
                send_message(
                    &self.socket,
                    PaxosMessage::RecoveryReply {
                        index: self.cluster_index,
                        payload: value,
                    },
                    &src_addr,
                )
                .await
                .unwrap();
                println!("Sent data request to follower at {}", src_addr);
            }
            Ok(None) => {
                println!("No value found");
            }
            Err(e) => {
                eprintln!("Error while reading from WAL: {}", e);
            }
        }
    }

    async fn broadcast_get_value(
        &self,
        follower_list: &Vec<String>,
        own_shard: &Option<Vec<u8>>,
        key: &str,
    ) -> Vec<Option<Vec<u8>>> {
        let mut recovery_shards: Vec<Option<Vec<u8>>> =
            vec![None; self.ec.shard_count + self.ec.parity_count];
        recovery_shards[self.cluster_index] = own_shard.clone();

        let mut tasks = JoinSet::new();
        let size = follower_list.len();

        for follower_addr in follower_list.iter() {
            let socket = Arc::clone(&self.socket);
            let key = key.to_string();
            let follower_addr = follower_addr.clone();

            tasks.spawn(async move {
                // Send the request to the follower
                if let Err(_e) = send_message(
                    &socket,
                    PaxosMessage::RecoveryRequest {
                        key: key.to_string(),
                    },
                    follower_addr.as_str(),
                )
                .await
                {
                    println!(
                        "Failed to broadcast request to follower at {}",
                        follower_addr
                    );
                    return None;
                }
                println!("Broadcasted request to follower at {}", follower_addr);

                // Wait for acknowledgment with timeout (ex. 2 seconds)
                match timeout(Duration::from_secs(2), receive_message(&socket)).await {
                    Ok(Ok((ack, _))) => {
                        if let PaxosMessage::RecoveryReply { index, payload } = ack {
                            println!("Received acknowledgment from {} ({})", follower_addr, index);
                            return Some((index, payload));
                        }
                    }
                    Ok(Err(e)) => {
                        println!(
                            "Error receiving acknowledgment from {}: {}",
                            follower_addr, e
                        );
                    }
                    Err(_) => {
                        println!("Timeout waiting for acknowledgment from {}", follower_addr);
                    }
                }
                return None;
            });
        }

        while let Some(response) = tasks.join_next().await {
            match response {
                Ok(Some((index, payload))) => {
                    if index < size {
                        recovery_shards[index] = Some(payload);
                    }
                }
                _ => {} // Handle errors or None responses if needed
            }
        }

        recovery_shards
    }
}
