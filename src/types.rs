use serde::{Deserialize, Serialize};

use crate::base_libs::operation::Operation;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FollowerRegistration {
    pub follower_addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PaxosMessage {
    // Paxos
    LeaderRequest {
        request_id: u64,
    },
    LeaderAccepted {
        request_id: u64,
        operation: Operation,
    },
    FollowerAck {
        request_id: u64,
    },
    FollowerRegister(FollowerRegistration),

    // Client requests
    ClientRequest {
        request_id: u64,
        payload: Vec<u8>,
    },
    RecoveryRequest {
        key: String,
    },
    RecoveryReply {
        payload: Vec<u8>,
    },
}
