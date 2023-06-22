use std::{thread, time::Duration};

use crate::{keys::Key, sled_storage::Storage};

mod interface;
mod keys;
mod sled_storage;

fn main() {
    let s = Storage::new("database.db");

    let key1 = Key([
        1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8,
    ]);
    let value1 = vec![9, 8, 7];
    let _ = s.put(key1, vec![1, 1, 1], false);
    thread::sleep(Duration::from_secs(2));
    let _ = s.put(key1, vec![2, 2, 2], true);
    thread::sleep(Duration::from_secs(2));
    let _ = s.put(key1, vec![3, 3, 3], true);
    thread::sleep(Duration::from_secs(2));
    let _ = s.put(key1, vec![4, 4, 4], true);
    thread::sleep(Duration::from_secs(2));
    let _ = s.put(key1, vec![5, 5, 5], true);
    thread::sleep(Duration::from_secs(2));
    let _ = s.put(key1, vec![6, 6, 6], true);
    thread::sleep(Duration::from_secs(2));
    let _ = s.put(key1, vec![7, 7, 7], true);

    let p = s.get_latest(key1);
    println!("RESULT: {:?}", p);
}
