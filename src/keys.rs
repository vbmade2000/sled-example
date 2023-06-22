/*
| KeyType | Key | SubKeyType | SubKey | timestamp |
| 1-byte  | u64 | 1-byte     |  u64   |    u64    |
examples:
____________________________________________________________
Key for the status of mission_id at timestamp
KeyType::Mission | mission_id | SubKey::Status | u64 (zeroed) |
timestamp
____________________________________________________________
*/
const KEY_SIZE: usize = 26;
pub type Value = Vec<u8>;
pub type KvPair = (Key, Value);
pub type KeyContainer = [u8; KEY_SIZE];
#[derive(Eq, PartialEq, Hash, Debug, Clone, Copy)]
pub struct Key(pub KeyContainer);
