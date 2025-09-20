use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::fmt::Debug;

#[derive(Debug, Serialize, Deserialize)]
pub enum Entry<K, V>
where
    K: Clone + Ord + Send + Sync + 'static + Debug,
    V: Clone + Send + 'static,
{
    Set {key: K, val: V},
    Rm {key: K},
}

#[derive(Clone, Debug)]
pub struct EntryOffset {
    pub file_id: u32,
    pub start: u64,
    pub end: u64,
}

impl<K, V> Entry<K, V>
where
    K: Clone + Serialize + DeserializeOwned + Ord + Send + Sync + 'static + Debug,
    V: Clone + Serialize + DeserializeOwned + Send + 'static,
{
    pub fn init_set(key: K, val: V) -> Entry<K, V> {
        Entry::Set{
            key,
            val,
        }
    }

    pub fn init_rm(key: K) -> Entry<K, V> {
        Entry::Rm{
            key,
        }
    }
}
