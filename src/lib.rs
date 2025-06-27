#![warn(missing_docs)]

//! Implemtation for the kvs crate

use std::collections::HashMap;

/// The store for kvs crate
pub struct KvStore {
    elements: HashMap<String, String>,
}

impl Default for KvStore {
    fn default() -> Self {
        Self::new()
    }
}

impl KvStore {
    /// ```
    /// # use kvs::KvStore;
    /// #
    /// # fn main() {
    /// let mut store = KvStore::new();
    /// # }
    /// ```
    pub fn new() -> Self {
        KvStore {
            elements: HashMap::new(),
        }
    }

    /// ```
    /// # use kvs::KvStore;
    /// #
    /// # fn main() {
    /// # let mut store = KvStore::new();
    /// # store.set("name".to_string(), "olamide".to_string());
    /// assert_eq!(store.get("name".to_string()), Some("olamide".to_string()));
    /// # }
    /// ```
    pub fn get(&self, key: String) -> Option<String> {
        return self.elements.get(&key).cloned();
    }

    /// ```
    /// # use kvs::KvStore;
    /// #
    /// # fn main() {
    /// # let mut store = KvStore::new();
    /// store.set("name".to_string(), "olamide".to_string());
    /// assert_eq!(store.get("name".to_string()), Some("olamide".to_string()));
    /// # }
    /// ```
    pub fn set(&mut self, key: String, value: String) {
        self.elements.insert(key, value);
    }

    /// ```
    /// # use kvs::KvStore;
    /// #
    /// # fn main() {
    /// # let mut store = KvStore::new();
    /// # store.set("name".to_string(), "olamide".to_string());
    /// store.remove("name".to_string());
    /// # assert_eq!(store.get("name".to_string()), None);
    /// # }
    /// ```
    pub fn remove(&mut self, key: String) {
        self.elements.remove(&key);
    }
}
