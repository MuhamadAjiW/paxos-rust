use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use tokio::net::UdpSocket;

use crate::{
    base_libs::{address::Address, paxos::PaxosState},
    network::receive_message,
    types::PaxosMessage,
};

use super::{erasure_coding::ECService, store::Store};

pub struct Node {
    // Base attributes
    pub address: Address,
    pub socket: UdpSocket,
    pub running: bool,

    // Paxos related attributes
    pub load_balancer_address: Address,
    pub leader_address: Address,
    pub followers: Arc<Mutex<HashSet<String>>>,
    pub state: PaxosState,
    pub request_id: u64,

    // Application attributes
    pub _store: Store,
    pub ec: ECService,
}

impl Node {
    pub async fn new(
        address: Address,
        leader_address: Address,
        load_balancer_address: Address,
        state: PaxosState,
        shard_count: usize,
        parity_count: usize,
    ) -> Self {
        let socket = UdpSocket::bind(address.to_string()).await.unwrap();
        let running = false;
        let followers = Arc::new(Mutex::new(HashSet::new()));
        let store = Store::new();
        let request_id = 0;
        let ec = ECService::new(shard_count, parity_count);

        return Node {
            address,
            socket,
            running,
            load_balancer_address,
            leader_address,
            followers,
            state,
            request_id,
            _store: store,
            ec,
        };
    }

    pub async fn run(&mut self) {
        println!("Starting node...");
        self.print_info();
        self.running = true;

        if matches!(self.state, PaxosState::Follower) {
            if !self.follower_send_register().await {
                return;
            }
        }

        while self.running {
            let (message, src_addr) = receive_message(&self.socket).await.unwrap();

            match message {
                PaxosMessage::LeaderRequest { request_id } => {
                    self.handle_leader_request(&src_addr, request_id).await
                }

                PaxosMessage::LeaderAccepted {
                    request_id,
                    payload,
                } => {
                    self.handle_leader_accepted(&src_addr, request_id, &payload)
                        .await
                }

                PaxosMessage::ClientRequest {
                    request_id,
                    payload,
                } => {
                    self.handle_client_request(&src_addr, request_id, &payload)
                        .await
                }

                PaxosMessage::FollowerAck { request_id } => {
                    self.handle_follower_ack(&src_addr, request_id).await
                }

                PaxosMessage::FollowerRegister(follower) => {
                    self.handle_follower_register(&src_addr, &follower).await
                }
            }
        }
    }

    pub fn print_info(&self) {
        println!("Node info:");
        println!("Address: {}", self.address.to_string());
        println!("Leader: {}", self.leader_address.to_string());
        println!("Load Balancer: {}", self.load_balancer_address.to_string());
        println!("State: {}", self.state.to_string());
    }
}
