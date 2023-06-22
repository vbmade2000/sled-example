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
    let r1 = s.put(key1, value1, false);

    // let key2 = Key([
    //     1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8,
    // ]);
    // let value2 = vec![6, 5, 4];
    // let r2 = s.put(key2, value2, false);

    // println!("Hello, world => {:?}", r1);
    // println!("Hello, world => {:?}", r2);
}
