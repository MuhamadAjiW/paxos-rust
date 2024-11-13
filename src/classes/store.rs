use std::collections::HashMap;

use crate::base_libs::request::{Request, RequestType};

use super::node::Node;

pub struct Store {
    map: HashMap<String, String>,
}

impl Store {
    pub fn new() -> Self {
        return Store {
            map: HashMap::new(),
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
    pub fn process_request(&mut self, request: &Request) -> String {
        let response;

        match request.reqtype {
            RequestType::BAD => {
                response = format!("Bad command received\n");
            }
            RequestType::PING => {
                response = format!("PONG\n");
            }
            RequestType::GET => {
                self.get_persistent();
                response = format!("{}\n", self.get(&request.key));
            }
            RequestType::SET => {
                self.set(&request.key.clone(), &request.val.clone());
                self.set_persistent();
                response = "OK\n".to_string();
            }
            RequestType::REMOVE => {
                self.remove(&request.key);
                self.set_persistent();
                response = "OK\n".to_string();
            }
        }

        response
    }

    // _TODO: Persistent data inside files
    pub fn get_persistent(&self) {}
    pub fn set_persistent(&self) {}
}

impl Node {}
