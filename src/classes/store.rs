use std::collections::HashMap;

pub struct Store {
    _map: HashMap<String, String>,
}

impl Store {
    pub fn new() -> Self {
        return Store {
            _map: HashMap::new(),
        };
    }

    // pub fn set(&mut self, key: &str, value: &str) {
    //     self.map.insert(key.to_string(), value.to_string());
    // }

    // pub fn get(&self, key: &str) -> String {
    //     return self.map.get(key).cloned().unwrap_or("".to_string());
    // }

    // pub fn remove(&mut self, key: &str) -> String {
    //     return self.map.remove(key).unwrap_or("".to_string());
    // }

    // _TODO: Persistent data inside files
    // pub fn get_persistent(&self) {}
    // pub fn set_persistent(&self) {}
}
