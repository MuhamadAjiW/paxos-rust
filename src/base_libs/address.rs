use core::fmt;

// ---AddressInput---
pub enum AddressInput {
    IpAndPort(String, u16),
    Address(Address),
}

// ---Address---
pub struct Address {
    pub ip: String,
    pub port: u16,
}
impl Address {
    pub fn new(ip: &str, port: u16) -> Self {
        return Address {
            ip: ip.to_string(),
            port: port,
        };
    }
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.ip, self.port)
    }
}
