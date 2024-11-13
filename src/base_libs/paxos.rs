use crate::{
    classes::node::Node,
    network::{receive_message, send_message},
    types::{FollowerRegistration, PaxosMessage},
};
use std::{fmt, time::Duration, u64};
use tokio::{net::UdpSocket, time::timeout};

// ---PaxosState---
pub enum PaxosState {
    Follower,
    Leader,
}
impl fmt::Display for PaxosState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PaxosState::Follower => write!(f, "Follower"),
            PaxosState::Leader => write!(f, "Leader"),
        }
    }
}

// _TODO: Implement handlers
// ---Node Commands---
impl Node {
    pub async fn handle_leader_request(&self, src_addr: String, request_id: u64) {
        match self.state {
            PaxosState::Follower => {}
            PaxosState::Leader => {}
        }
    }
    pub async fn handle_leader_accepted(
        &self,
        src_addr: String,
        request_id: u64,
        payload: Vec<u8>,
    ) {
        match self.state {
            PaxosState::Follower => {}
            PaxosState::Leader => {}
        }
    }
    pub async fn handle_client_request(&self, src_addr: String, request_id: u64, payload: Vec<u8>) {
        match self.state {
            PaxosState::Follower => {}

            PaxosState::Leader => {}
        }
    }
    pub async fn handle_follower_ack(&self, src_addr: String, request_id: u64) {
        match self.state {
            PaxosState::Follower => {}
            PaxosState::Leader => {}
        }
    }
    pub async fn handle_follower_register(
        &self,
        src_addr: String,
        follower: &FollowerRegistration,
    ) {
        match self.state {
            PaxosState::Follower => {}
            PaxosState::Leader => {}
        }
    }
}

// _TODO: these should be class functions, but will do for now
// ---Temp---
pub async fn broadcast_prepare(
    follower_list: &Vec<String>,
    socket: &UdpSocket,
    request_id: u64,
) -> bool {
    if follower_list.is_empty() {
        println!("No followers registered. Cannot proceed.");
        return false;
    }

    let mut acks = 0;
    let majority = follower_list.len() / 2 + 1;

    // Use a timeout for each follower acknowledgment
    for follower_addr in follower_list {
        // Send the request to the follower
        send_message(
            &socket,
            PaxosMessage::LeaderRequest { request_id },
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
        match timeout(Duration::from_secs(2), receive_message(&socket)).await {
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

        if acks >= majority {
            println!("Leader received enough acknowledgment");
            return true;
        }
    }

    // If majority is reached, respond to load balancer immediately
    return acks >= majority;
}

pub async fn broadcast_accept(
    follower_list: &Vec<String>,
    socket: &UdpSocket,
    request_id: u64,
    encoded_shard: &Vec<Vec<u8>>,
) -> bool {
    if follower_list.is_empty() {
        println!("No followers registered. Cannot proceed.");
        return false;
    }

    let mut acks = 0;

    // Use a timeout for each follower acknowledgment
    for (index, follower_addr) in follower_list.iter().enumerate() {
        // Send the request to the follower
        send_message(
            &socket,
            PaxosMessage::LeaderAccepted {
                request_id,
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
        match timeout(Duration::from_secs(2), receive_message(&socket)).await {
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

    // Every node should successfully received the accept message
    return acks == follower_list.len();
}
