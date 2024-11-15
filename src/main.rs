use std::io;

use base_libs::{address::Address, paxos::PaxosState};
use classes::node::Node;

mod base_libs;
mod classes;
mod load_balancer;
mod network;
mod types;

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    let role = std::env::args().nth(1).expect("No role provided");
    let shard_count = 2;
    let parity_count = 1;
    let ec_active = true;

    if role == "leader" {
        let address = Address::new("127.0.0.1", 8080);
        let leader_address = Address::new("127.0.0.1", 8080);
        let load_balancer_address = Address::new("127.0.0.1", 8000);
        let mut node = Node::new(
            address,
            leader_address,
            load_balancer_address,
            PaxosState::Leader,
            shard_count,
            parity_count,
            ec_active,
        )
        .await;

        node.run().await?;
    } else if role == "follower" {
        let follower_addr_input = std::env::args()
            .nth(2)
            .expect("No follower address provided");
        let leader_addr_input = std::env::args().nth(3).expect("No leader address provided");
        let load_balancer_addr_input = std::env::args()
            .nth(4)
            .expect("No load balancer address provided");

        let address = Address::from_string(&follower_addr_input).unwrap();
        let leader_address = Address::from_string(&leader_addr_input).unwrap();
        let load_balancer_address = Address::from_string(&load_balancer_addr_input).unwrap();
        let mut node = Node::new(
            address,
            leader_address,
            load_balancer_address,
            PaxosState::Follower,
            shard_count,
            parity_count,
            ec_active,
        )
        .await;

        node.run().await?;
    } else if role == "load_balancer" {
        let mut lb = load_balancer::LoadBalancer::new(); // Declare lb as mutable
        lb.listen_and_route("127.0.0.1:8000").await; // Call listen_and_route with mutable reference
    } else {
        panic!("Invalid role! Use 'leader', 'follower', or 'load_balancer'.");
    }

    Ok(())
}
