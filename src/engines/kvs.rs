use super::{store, KvsEngine};
use crate::entry::Entry;
use crate::error::Result;
use std::path::Path;
use std::fs;
use serde::{Serialize, de::DeserializeOwned};
use std::fmt::Debug;


#[derive(Clone)]
pub struct KvStore<K, V>
where
    K: Clone + Serialize + DeserializeOwned + Ord + Send + Sync + 'static + Debug,
    V: Clone + Serialize + DeserializeOwned + Send + 'static,
{
    store: store::Store<K, V>,
}

impl<K, V> KvStore<K, V>
where
    K: Clone + Serialize + DeserializeOwned + Ord + Send + Sync + 'static + Debug,
    V: Clone + Serialize + DeserializeOwned + Send + 'static,
{
    pub fn open(dir: &Path) -> Result<KvStore<K, V>> {
        let _ = fs::create_dir_all(dir);
        let store = store::Store::new(dir)?;

        Ok(KvStore{
            store,
        })
    }
}

impl<K, V> KvsEngine<K, V> for KvStore<K, V>
where
    K: Clone + Serialize + DeserializeOwned + Ord + Send + Sync + 'static + Debug,
    V: Clone + Serialize + DeserializeOwned + Send + 'static,
{
    fn set(&self, key: K, val: V) -> Result<()> {
        let cmd = Entry::init_set(key.clone(), val.clone());
        let serialized = serde_json::to_string(&cmd).unwrap();
        let b = serialized.as_bytes();

        self.store.write(key, b)
    }

    fn remove(&self, key: K) -> Result<K> {
        self.store.remove(key.clone())?;

        Ok(key)
    }

    fn get(&self, key: K) -> Result<Option<V>> {
        if !self.store.index.contains_key(&key) {
            return Ok(None);
        }

        let offset = self.store.index.get(&key).unwrap();
        Ok(self.store.read(offset.value().file_id, offset.value().start, offset.value().end)?)
    }
}
