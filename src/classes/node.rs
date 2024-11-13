use tokio::net::UdpSocket;

use crate::{
    base_libs::{
        address::{Address, AddressInput},
        paxos::PaxosState,
    },
    network::receive_message,
    types::PaxosMessage,
};

use super::store::Store;

pub struct Node {
    // Base attributes
    pub address: Address,
    pub socket: UdpSocket,
    pub running: bool,

    // Paxos related attributes
    pub load_balancer_address: Address,
    pub leader_address: Address,
    pub follower_list: Vec<Address>,
    pub state: PaxosState,

    // Application attributes
    pub store: Store,
}

impl Node {
    pub async fn new(
        addr_input: AddressInput,
        leader_addr_input: AddressInput,
        load_balancer_addr_input: AddressInput,
        state: PaxosState,
    ) -> Self {
        let address: Address = Address::get(addr_input);
        let leader_address: Address = Address::get(leader_addr_input);
        let load_balancer_address: Address = Address::get(load_balancer_addr_input);
        let socket = UdpSocket::bind(address.to_string()).await.unwrap();
        let running = false;
        let follower_list = vec![];
        let store = Store::new();

        return Node {
            address,
            socket,
            running,
            load_balancer_address,
            leader_address,
            follower_list,
            state,
            store,
        };
    }

    pub async fn run(&mut self) {
        self.running = true;

        while self.running {
            let (message, src_addr) = receive_message(&self.socket).await.unwrap();

            match message {
                PaxosMessage::LeaderRequest { request_id } => {
                    self.handle_leader_request(src_addr, request_id).await
                }
                PaxosMessage::LeaderAccepted {
                    request_id,
                    payload,
                } => {
                    self.handle_leader_accepted(src_addr, request_id, payload)
                        .await
                }
                PaxosMessage::ClientRequest {
                    request_id,
                    payload,
                } => {
                    self.handle_client_request(src_addr, request_id, payload)
                        .await
                }
                PaxosMessage::FollowerAck { request_id } => {
                    self.handle_follower_ack(src_addr, request_id).await
                }
                PaxosMessage::FollowerRegister(follower) => {
                    self.handle_follower_register(src_addr, &follower).await
                }
            }
        }
    }

    pub fn print_info(&self) {
        println!("Node info:");
        println!("Address: {}", self.address.to_string());
        println!("State: {}", self.state.to_string());
    }
}
