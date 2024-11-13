use crate::base_libs::paxos::{broadcast_accept, broadcast_prepare};
use crate::classes::erasure_coding::ECService;
use crate::network::receive_message;
use crate::types::PaxosMessage;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use tokio::net::UdpSocket;
use tokio::time::{sleep, Duration}; // For retrying and timeout

pub async fn leader_main(leader_addr: &str, load_balancer_addr: &str) {
    let ec = ECService::new(4, 2);
    let socket = UdpSocket::bind(leader_addr).await.unwrap();

    // Retry logic for registering with load balancer
    let lb_registration_message = format!("register:{}", leader_addr);
    let mut registered = false;
    while !registered {
        match socket
            .send_to(lb_registration_message.as_bytes(), load_balancer_addr)
            .await
        {
            Ok(_) => {
                println!(
                    "Leader registered with load balancer: {}",
                    load_balancer_addr
                );
                registered = true; // Registration successful
            }
            Err(e) => {
                println!(
                    "Failed to register with load balancer, retrying in 2 seconds: {}",
                    e
                );
                sleep(Duration::from_secs(2)).await; // Retry after 2 seconds
            }
        }
    }

    let followers = Arc::new(Mutex::new(HashSet::new()));

    loop {
        let (message, _src_addr) = receive_message(&socket).await.unwrap();

        match message {
            PaxosMessage::RegisterFollower(follower) => {
                let mut followers_guard = followers.lock().unwrap();
                followers_guard.insert(follower.follower_addr.clone());
                println!("Follower registered: {}", follower.follower_addr);
            }
            PaxosMessage::ClientRequest {
                request_id,
                payload,
            } => {
                let original_message = String::from_utf8_lossy(&payload).to_string(); // Capture original client message
                println!("Leader received request from client: {}", original_message);

                let payload_encoded = ec.encode(&payload);

                let follower_list: Vec<String> = {
                    let followers_guard = followers.lock().unwrap();
                    followers_guard.iter().cloned().collect()
                };

                if broadcast_prepare(&follower_list, &socket, request_id).await {
                    println!(
                        "Leader received majority acknowledgment, responding to load balancer at {}",
                        load_balancer_addr
                    );
                    let response = format!(
                        "Request ID: {}\nMessage received enough acknowledgements.\n",
                        request_id,
                    );
                    socket
                        .send_to(response.as_bytes(), load_balancer_addr)
                        .await
                        .unwrap();

                    if broadcast_accept(&follower_list, &socket, request_id, &payload_encoded).await
                    {
                        println!("All followers accepted.",);
                    } else {
                        println!("Not all followers accepted.",);
                    }
                } else {
                    println!("Not enough acknowledgments to proceed.",);
                    let response = format!(
                        "Request ID: {}\nNot enough acknowledgments to proceed.",
                        request_id,
                    );
                    socket
                        .send_to(response.as_bytes(), load_balancer_addr)
                        .await
                        .unwrap();
                }

                // if follower_list.is_empty() {
                //     println!("No followers registered. Cannot proceed.");
                //     continue;
                // }

                // let mut acks = 0;
                // let majority = follower_list.len() / 2 + 1;

                // // Use a timeout for each follower acknowledgment
                // for (index, follower_addr) in follower_list.iter().enumerate() {
                //     // Send the request to the follower
                //     send_message(
                //         &socket,
                //         PaxosMessage::ClientRequest {
                //             request_id,
                //             // payload: payload.clone(),
                //             payload: payload_encoded[index].clone(),
                //         },
                //         follower_addr,
                //     )
                //     .await
                //     .unwrap();
                //     println!(
                //         "Leader broadcasted request to follower at {}",
                //         follower_addr
                //     );

                //     // Wait for acknowledgment with timeout (ex. 2 seconds)
                //     match timeout(Duration::from_secs(2), receive_message(&socket)).await {
                //         Ok(Ok((ack, _))) => {
                //             if let PaxosMessage::FollowerAck { .. } = ack {
                //                 acks += 1;
                //                 println!(
                //                     "Leader received acknowledgment from follower at {}",
                //                     follower_addr
                //                 );
                //             }
                //         }
                //         Ok(Err(e)) => {
                //             println!(
                //                 "Error receiving acknowledgment from follower at {}: {}",
                //                 follower_addr, e
                //             );
                //         }
                //         Err(_) => {
                //             println!(
                //                 "Timeout waiting for acknowledgment from follower at {}",
                //                 follower_addr
                //             );
                //         }
                //     }

                //     // If majority is reached, respond to load balancer immediately
                //     if acks >= majority {
                //         println!("Leader received majority acknowledgment, responding to load balancer at {}", load_balancer_addr);

                //         let response = format!(
                //             "Request ID: {}\nOriginal Message: {}\nAcknowledgments Received: {}\nTotal Followers: {}\n",
                //             request_id, original_message, acks, follower_list.len()
                //         );
                //         socket
                //             .send_to(response.as_bytes(), load_balancer_addr)
                //             .await
                //             .unwrap();
                //         break; // Stop once majority is reached
                //     }
                // }

                // if acks < majority {
                //     println!(
                //         "Not enough acknowledgments to proceed (Received: {}, Majority: {}).",
                //         acks, majority
                //     );
                //     let response = format!(
                //         "Request ID: {}\nNot enough acknowledgments to proceed (Received: {}, Majority: {}).",
                //         request_id, acks, majority
                //     );
                //     socket
                //         .send_to(response.as_bytes(), load_balancer_addr)
                //         .await
                //         .unwrap();
                //     break;
                // }
            }
            _ => {}
        }
    }
}
