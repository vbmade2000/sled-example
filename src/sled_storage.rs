use crate::interface::StorageResult;
use crate::keys::{Key, KvPair, Value};
use sled::{self, IVec};
use sled::{Batch, Db};
use std::path::Path;

#[derive(Clone, Debug)]
pub struct Storage {
    engine: Db,
}
impl Storage {
    pub fn new<P: AsRef<Path>>(path: P) -> Storage {
        let config = sled::Config::default()
            .path(path)
            .cache_capacity(5 * 1024 * 1024 * 1024)
            .mode(sled::Mode::HighThroughput)
            .use_compression(false)
            .temporary(false)
            .flush_every_ms(Some(1000));
        let db = config.open().unwrap();
        Self { engine: db }
    }

    /// Get the value at key and returns it
    // fn get(&self, key: Key) -> StorageResult<Value> {

    //     // Implement here
    // }
    // /// Fetch from db data but ignore timestamp in key and always get the latest timestamp
    // fn get_latest(&self, key: Key) // -> ??
    // {
    //     // Implement here
    // }
    /// If key exists insert a new version (if autoincrement is true)
    pub fn put(&self, key: Key, value: Value, _autoincrement: bool) -> StorageResult<Option<IVec>> {
        let key = key.0;

        /* Simple version */
        // TODO: Check if compare_and_swap() can be used.
        let existing_key = self.engine.get(key);
        match existing_key {
            Ok(k) => match k {
                Some(_) => {
                    if _autoincrement {
                        return Ok(self.engine.insert(key, IVec::from(value))?);
                    }
                    // Keep the old version.
                    return Ok(None);
                }
                None => {
                    return Ok(self.engine.insert(key, IVec::from(value))?);
                }
            },
            Err(e) => return Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_put_nonexisting_true() {
        // TODO: 1. Create new database everytime and create them in temporary dir.
        // TODO: 2. Create separate function to create database.

        let s = Storage::new("test_database1.db");

        // Input: Non-Existing Key, autoincrement=true

        // Preparation
        let key1 = Key([
            1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8,
        ]);
        let value1 = vec![9, 8, 7];

        // Call
        let result = s.put(key1, value1, false);

        // Assertion
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_put_existing_true() {
        let s = Storage::new("test_database2.db");

        // Input: Existing Key, autoincrement=true

        // Preparation
        let key1 = Key([
            1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8,
        ]);
        let value1 = vec![9, 8, 7];
        let value2 = vec![5, 6, 7];
        let _ = s.put(key1, value1.clone(), false);

        // Call
        let result = s.put(key1, value2.clone(), true);

        // Assertion
        assert_eq!(result.unwrap().unwrap(), IVec::from(value1));
    }

    #[test]
    fn test_put_nonexisting_false() {
        let s = Storage::new("test_database3.db");

        // Input: Non-Existing Key, autoincrement=false

        // Preparation
        let key1 = Key([
            1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8,
        ]);
        let value1 = vec![9, 8, 7];

        // Call
        let result = s.put(key1, value1.clone(), false);

        // Assertion
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_put_existing_false() {
        let s = Storage::new("test_database4.db");

        // Input: Existing Key, autoincrement=false

        // Preparation
        let key1 = Key([
            1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8,
        ]);
        let value1 = vec![9, 8, 7];
        let value2 = vec![5, 6, 7];
        let _ = s.put(key1, value1.clone(), false);

        // Call
        let result = s.put(key1, value2.clone(), false);

        // Assertion
        assert_eq!(result.unwrap(), None);
    }
}
