use crate::Result;

pub trait KvsEngine {
    fn get(&mut self, key: String) -> Result<Option<String>>;
    fn set(&mut self, key: String, val: String) -> Result<()>;
    fn remove(&mut self, key: String) -> Result<String>;
}

mod kvs;
mod store;
mod sled;

pub use self::kvs::KvStore;
pub use self::sled::SledKvsEngine;
