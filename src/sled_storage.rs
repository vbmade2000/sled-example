use crate::interface::{StorageError, StorageResult};
use crate::keys::{Key, KvPair, Value};
use sled::{self, IVec};
use sled::{Batch, Db};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

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

    fn get_current_timestamp(&self) -> Vec<u8> {
        let start = SystemTime::now();
        start
            .duration_since(UNIX_EPOCH)
            .expect("Error in retrieving current timestamp")
            .as_secs()
            .to_le_bytes()
            .to_vec()
    }

    /// Get the value at key and returns it
    fn get(&self, key: Key) -> StorageResult<Value> {
        // Fetch value
        let value = self.engine.get(key.0)?;

        match value {
            Some(v) => Ok(v.to_vec()),
            None => Err(StorageError::KeyNotFound {
                key: key,
                start_key: key,
                end_key: key,
            }),
        }
    }
    // /// Fetch from db data but ignore timestamp in key and always get the latest timestamp
    // fn get_latest(&self, key: Key) // -> ??
    // {
    //     // Implement here
    // }
    /// If key exists insert a new version (if autoincrement is true)
    pub fn put(
        &self,
        key: Key,
        value: Value,
        _autoincrement: bool,
    ) -> StorageResult<Option<Value>> {
        // TODO: Raise error if key length is more than X.

        let key = key.0;

        // Modify key to add current timestamp
        let mut new_key = key[0..18].to_vec();
        let timesmap = self.get_current_timestamp();
        new_key.extend(timesmap);

        /* Simple version */
        // TODO: Check if compare_and_swap() can be used.
        let existing_key = self.engine.get(key);
        match existing_key {
            Ok(k) => match k {
                Some(_) => {
                    if _autoincrement {
                        // This is duplication of code. Can be extracted to a separate function.
                        let result = self.engine.insert(new_key, IVec::from(value))?;
                        match result {
                            Some(v) => return Ok(Some(v.to_vec())),
                            None => return Ok(None),
                        }
                    }
                    // Keep the old version.
                    return Ok(None);
                }
                None => {
                    // This is duplication of code. Can be extracted to a separate function.
                    let result = self.engine.insert(new_key, IVec::from(value))?;
                    match result {
                        Some(v) => Ok(Some(v.to_vec())),
                        None => Ok(None),
                    }
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
        assert_eq!(result.unwrap().unwrap(), value1);
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

    #[test]
    fn test_get_key_exist() {
        let s = Storage::new("test_database5.db");

        // Input: Existing Key

        // Preparation
        let key = Key([
            1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8,
        ]);
        let value = vec![9, 8, 7];
        let _ = s.put(key, value.clone(), false);

        // Call
        let result = s.get(key);

        // Assertion
        assert_eq!(result.unwrap(), value);
    }

    #[test]
    fn test_get_key_nonexistant() {
        let s = Storage::new("test_database6.db");

        // Input: Non-Existing Key

        // Preparation
        let some_non_existant_key = Key([
            1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8,
        ]);
        // let _ = s.put(key, value.clone(), false);

        // Call
        let result = s.get(some_non_existant_key);

        // Assertion
        assert_eq!(result.is_err(), true);

        // This requires [derive(PartialEq)] for StorageError.
        // assert_eq!(result.err().unwrap(), StorageError::KeyNotFound { key: key, start_key: key, end_key: key });
    }
}
