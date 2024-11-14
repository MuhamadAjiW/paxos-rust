use core::fmt;

#[derive(PartialEq)]
pub enum OperationType {
    BAD,
    PING,
    GET,
    SET,
    DELETE,
}

// ---RequestType---
impl fmt::Display for OperationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let str;
        match self {
            OperationType::BAD => str = "BAD",
            OperationType::PING => str = "PING",
            OperationType::GET => str = "GET",
            OperationType::SET => str = "SET",
            OperationType::DELETE => str = "DEL",
        }

        write!(f, "{}", str)
    }
}

pub struct Operation {
    pub op_type: OperationType,
    pub key: String,
    pub val: String,
}

// ---Operation---
impl Operation {
    pub fn parse(payload: &Vec<u8>) -> Option<Self> {
        let payload_str = String::from_utf8(payload.clone()).ok()?;
        let command = payload_str.trim();
        let parts: Vec<&str> = command.split_whitespace().collect();

        if parts.is_empty() {
            return None;
        }

        match parts.as_slice() {
            ["PING"] => {
                return Some(Operation {
                    op_type: OperationType::PING,
                    key: "".to_string(),
                    val: "".to_string(),
                })
            }
            ["GET", key] => {
                return Some(Operation {
                    op_type: OperationType::GET,
                    key: key.to_string(),
                    val: "".to_string(),
                })
            }
            ["SET", key, val] => {
                return Some(Operation {
                    op_type: OperationType::SET,
                    key: key.to_string(),
                    val: val.to_string(),
                })
            }
            ["DEL", key] => {
                return Some(Operation {
                    op_type: OperationType::DELETE,
                    key: key.to_string(),
                    val: "".to_string(),
                })
            }
            _ => {
                return Some(Operation {
                    op_type: OperationType::BAD,
                    key: "".to_string(),
                    val: "".to_string(),
                })
            }
        }
    }
}

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {} {}", self.op_type, self.key, self.val)
    }
}
