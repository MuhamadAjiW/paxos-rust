use std::time::Duration;

use tokio::time::timeout;

use crate::{
    base_libs::operation::Operation,
    classes::{node::Node, store::BinKV},
    network::{receive_message, send_message},
    types::{FollowerRegistration, PaxosMessage},
};

impl Node {
    // ---Active Commands---
    async fn leader_broadcast_prepare(&self, follower_list: &Vec<String>) -> usize {
        if follower_list.is_empty() {
            println!("No followers registered. Cannot proceed.");
            return 0;
        }

        let mut acks = 0;

        for follower_addr in follower_list {
            // Send the request to the follower
            send_message(
                &self.socket,
                PaxosMessage::LeaderRequest {
                    request_id: self.request_id,
                },
                &follower_addr,
            )
            .await
            .unwrap();
            println!(
                "Leader broadcasted request to follower at {}",
                follower_addr
            );

            // _TODO: Should not be blocking, use state management inside the class instead
            // Wait for acknowledgment with timeout (ex. 2 seconds)
            match timeout(Duration::from_secs(2), receive_message(&self.socket)).await {
                Ok(Ok((ack, _))) => {
                    if let PaxosMessage::FollowerAck { .. } = ack {
                        acks += 1;
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

        acks
    }

    async fn leader_broadcast_accept_ec(
        &self,
        follower_list: &Vec<String>,
        encoded_shard: &Vec<Vec<u8>>,
    ) -> usize {
        if follower_list.is_empty() {
            println!("No followers registered. Cannot proceed.");
            return 0;
        }

        let mut acks = 0;

        for (index, follower_addr) in follower_list.iter().enumerate() {
            // Send the request to the follower
            send_message(
                &self.socket,
                PaxosMessage::LeaderAccepted {
                    request_id: self.request_id,
                    payload: encoded_shard[index].clone(),
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
                    if let PaxosMessage::FollowerAck { .. } = ack {
                        acks += 1;
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

        acks
    }

    async fn leader_broadcast_accept_replication(
        &self,
        follower_list: &Vec<String>,
        encoded_shard: &Vec<u8>,
    ) -> usize {
        if follower_list.is_empty() {
            println!("No followers registered. Cannot proceed.");
            return 0;
        }

        let mut acks = 0;

        for follower_addr in follower_list {
            // Send the request to the follower
            send_message(
                &self.socket,
                PaxosMessage::LeaderAccepted {
                    request_id: self.request_id,
                    payload: encoded_shard.clone(),
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
                    if let PaxosMessage::FollowerAck { .. } = ack {
                        acks += 1;
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

        acks
    }

    // ---Handlers---
    pub async fn leader_handle_client_request(
        &mut self,
        src_addr: &String,
        _request_id: u64,
        payload: &Vec<u8>,
    ) {
        let original_message = String::from_utf8_lossy(&payload).to_string(); // Capture original client message
        println!(
            "Leader received request from {}: {}",
            src_addr, original_message
        );
        let req = Operation::parse(payload);

        if matches!(req, None) {
            println!("Request was invalid, dropping request");
            return;
        }

        let load_balancer_addr = &self.load_balancer_address.to_string() as &str;
        let initial_request_id = self.request_id;

        let follower_list: Vec<String> = {
            let followers_guard = self.followers.lock().unwrap();
            followers_guard.iter().cloned().collect()
        };

        let majority = follower_list.len() / 2 + 1;
        let mut acks = self.leader_broadcast_prepare(&follower_list).await;
        let mut result: String;
        let message: &str;

        if acks >= majority {
            let operation = req.unwrap();
            result = self.store.process_request(&operation);

            // TODO: Implement
            // if result.is_empty() {
            //     result = self.get_from_cluster(&operation.key).await.expect("Failed to get from cluster");
            // }

            if self.ec_active {
                let encoded_shard = self.ec.encode(&payload);
                let kv = BinKV {
                    key: operation.key,
                    value: encoded_shard[0].clone(),
                };
                let _ = self.store.persist_request_ec(&operation.op_type, &kv).await;

                acks = self
                    .leader_broadcast_accept_ec(&follower_list, &encoded_shard)
                    .await;
            } else {
                acks = self
                    .leader_broadcast_accept_replication(&follower_list, &payload)
                    .await;
            }

            if acks > majority {
                message = "Leader broadcasted the message successfully";
            } else {
                message = "Accept broadcast is not accepted by majority";
            }

            self.request_id += 1;
        } else {
            result = "Request failed.".to_string();
            message = "Request broadcast is not accepted by majority";
        }

        let response = format!(
            "Request ID: {}\nMessage: {}\nReply: {}.",
            initial_request_id, message, result
        );
        self.socket
            .send_to(response.as_bytes(), load_balancer_addr)
            .await
            .unwrap();
    }

    pub async fn leader_handle_follower_register(&self, follower: &FollowerRegistration) {
        let mut followers_guard = self.followers.lock().unwrap();
        followers_guard.insert(follower.follower_addr.clone());
        println!("Follower registered: {}", follower.follower_addr);
    }

    // _TODO: handle false leader
    pub async fn leader_handle_leader_request(&self, _src_addr: &String, _request_id: u64) {}
    pub async fn leader_handle_leader_accepted(
        &self,
        _src_addr: &String,
        _request_id: u64,
        _payload: &Vec<u8>,
    ) {
    }
    // _TODO: handle false message
    pub async fn leader_handle_follower_ack(&self, _src_addr: &String, _request_id: u64) {}
}
