use crate::Result;
use serde::{Serialize, de::DeserializeOwned};
use std::fmt::Debug;

pub trait KvsEngine<K, V>: Clone + Send + 'static
where
    K: Clone + Serialize + DeserializeOwned + Ord + Send + Sync + 'static + Debug,
    V: Clone + Serialize + DeserializeOwned + Send + 'static,
{
    fn get(&self, key: K) -> Result<Option<V>>;
    fn set(&self, key: K, val: V) -> Result<()>;
    fn remove(&self, key: K) -> Result<K>;
}

mod kvs;
mod store;

pub use self::kvs::KvStore;
