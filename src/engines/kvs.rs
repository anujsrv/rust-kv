use super::{store, KvsEngine};
use crate::entry::Entry;
use crate::error::Result;
use std::path::Path;
use std::fs;


#[derive(Clone)]
pub struct KvStore {
    store: store::Store,
}

impl KvStore {
    pub fn open(dir: &Path) -> Result<KvStore> {
        let _ = fs::create_dir_all(dir);
        let store = store::Store::new(dir)?;

        Ok(KvStore{
            store,
        })
    }
}

impl KvsEngine for KvStore {
    fn set(&self, key: String, val: String) -> Result<()> {
        let cmd = Entry::init_set(key.clone(), val.clone());
        let serialized = serde_json::to_string(&cmd).unwrap();
        let b = serialized.as_bytes();

        self.store.write(key, b)
    }

    fn remove(&self, key: String) -> Result<String> {
        self.store.remove(key.clone())?;

        Ok(key)
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        if !self.store.index.contains_key(&key) {
            return Ok(None);
        }
        
        let offset = self.store.index.get(&key).unwrap();
        Ok(self.store.read(offset.value().file_id, offset.value().start, offset.value().end)?)
    }
}
