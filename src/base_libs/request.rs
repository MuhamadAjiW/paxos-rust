use core::fmt;

#[derive(PartialEq)]
pub enum RequestType {
    BAD,
    GET,
    SET,
    REMOVE,
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
    pub fn parse(payload: &Vec<u8>) -> Option<Self> {
        let payload_str = String::from_utf8(payload.clone()).ok()?;
        let command = payload_str.trim();
        let parts: Vec<&str> = command.split_whitespace().collect();

        if parts.is_empty() {
            return None;
        }

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
