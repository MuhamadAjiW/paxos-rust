use core::fmt;
use std::{
    io::{BufRead, BufReader},
    net::TcpStream,
};

#[derive(PartialEq)]
pub enum RequestType {
    BAD,
    GET,
    SET,
    REMOVE,
    TERMINATE,
}

// ---RequestType---
impl fmt::Display for RequestType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let str;
        match self {
            RequestType::BAD => str = "BAD",
            RequestType::GET => str = "GET",
            RequestType::SET => str = "SET",
            RequestType::REMOVE => str = "REMOVE",
            RequestType::TERMINATE => str = "TERMINATE",
        }

        write!(f, "{}", str)
    }
}

pub struct Request {
    pub reqtype: RequestType,
    pub key: String,
    pub val: String,
}

// ---Request---
impl Request {
    pub fn parse(stream: &mut TcpStream) -> Option<Self> {
        let mut reader =
            BufReader::new(stream.try_clone().expect("Request: Failed to clone stream"));

        let mut command_line = String::new();
        match reader.read_line(&mut command_line) {
            Ok(0) => {
                return None;
            }
            Err(_) => {
                return Some(Request {
                    reqtype: RequestType::BAD,
                    key: "".to_string(),
                    val: "".to_string(),
                })
            }
            Ok(_) => {}
        }

        let command = command_line.trim();
        let parts: Vec<&str> = command.split_whitespace().collect();

        match parts.as_slice() {
            ["GET", key] => {
                return Some(Request {
                    reqtype: RequestType::GET,
                    key: key.to_string(),
                    val: "".to_string(),
                })
            }
            ["SET", key, val] => {
                return Some(Request {
                    reqtype: RequestType::SET,
                    key: key.to_string(),
                    val: val.to_string(),
                })
            }
            ["REMOVE", key] => {
                return Some(Request {
                    reqtype: RequestType::REMOVE,
                    key: key.to_string(),
                    val: "".to_string(),
                })
            }
            ["EXIT"] => {
                return Some(Request {
                    reqtype: RequestType::TERMINATE,
                    key: "".to_string(),
                    val: "".to_string(),
                })
            }
            _ => {
                return Some(Request {
                    reqtype: RequestType::BAD,
                    key: "".to_string(),
                    val: "".to_string(),
                })
            }
        }
    }
}

impl fmt::Display for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {} {}", self.reqtype, self.key, self.val)
    }
}
