use std::{thread::sleep, time::Duration};

use crate::{
    classes::node::Node,
    network::send_message,
    types::{FollowerRegistration, PaxosMessage},
};

impl Node {
    // ---Active Commands---
    pub async fn follower_send_register(&self) -> bool {
        let leader_addr = &self.leader_address.to_string() as &str;
        let follower_addr = &self.address.to_string() as &str;
        let load_balancer_addr = &self.load_balancer_address.to_string() as &str;

        // Register with the leader
        let registration_message = PaxosMessage::FollowerRegister(FollowerRegistration {
            follower_addr: follower_addr.to_string(),
        });
        send_message(&self.socket, registration_message, leader_addr)
            .await
            .unwrap();
        println!("Follower registered with leader: {}", leader_addr);

        // Retry logic for registering with load balancer
        let lb_registration_message = format!("register:{}", follower_addr);
        let mut registered = false;
        while !registered {
            match self
                .socket
                .send_to(lb_registration_message.as_bytes(), load_balancer_addr)
                .await
            {
                Ok(_) => {
                    println!(
                        "Follower registered with load balancer: {}",
                        load_balancer_addr
                    );
                    registered = true; // Registration successful
                }
                Err(e) => {
                    println!(
                        "Failed to register with load balancer, retrying in 2 seconds: {}",
                        e
                    );
                    sleep(Duration::from_secs(2)); // Retry after 2 seconds
                }
            }
        }

        return registered;
    }

    // ---Handlers---
    pub async fn follower_handle_leader_request(&self, src_addr: &String, request_id: u64) {
        let leader_addr = &self.leader_address.to_string() as &str;
        if src_addr != leader_addr {
            println!("Follower received request message from not a leader");
            return;
        }

        println!("Follower received request message from leader");
        let ack = PaxosMessage::FollowerAck { request_id };
        send_message(&self.socket, ack, &leader_addr).await.unwrap();
        println!("Follower acknowledged request ID: {}", request_id);
    }
    pub async fn follower_handle_leader_accepted(
        &self,
        src_addr: &String,
        request_id: u64,
        payload: &Vec<u8>,
    ) {
        let leader_addr = &self.leader_address.to_string() as &str;
        if src_addr != leader_addr {
            println!("Follower received request message from not a leader");
            return;
        }

        println!(
            "Follower received accept message from leader: {:?}",
            payload
        );
        let ack = PaxosMessage::FollowerAck { request_id };
        send_message(&self.socket, ack, &leader_addr).await.unwrap();
        println!("Follower acknowledged request ID: {}", request_id);
    }
    pub async fn follower_handle_client_request(
        &self,
        src_addr: &String,
        request_id: u64,
        payload: &Vec<u8>,
    ) {
        let leader_addr = &self.leader_address.to_string() as &str;
        if src_addr != leader_addr {
            println!("Follower received client request. Forwarding to leader.");
            send_message(
                &self.socket,
                PaxosMessage::ClientRequest {
                    request_id,
                    payload: payload.clone(),
                },
                leader_addr,
            )
            .await
            .unwrap();
        }
    }

    // _TODO: handle false message
    pub async fn follower_handle_follower_ack(&self, _src_addr: &String, _request_id: u64) {}
    pub async fn follower_handle_follower_register(
        &self,
        _src_addr: &String,
        _follower: &FollowerRegistration,
    ) {
    }
}
