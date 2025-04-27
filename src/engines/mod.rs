use crate::Result;

pub trait KvsEngine: Clone + Send + 'static {
    fn get(&self, key: String) -> Result<Option<String>>;
    fn set(&self, key: String, val: String) -> Result<()>;
    fn remove(&self, key: String) -> Result<String>;
}

mod kvs;
mod store;

pub use self::kvs::KvStore;
