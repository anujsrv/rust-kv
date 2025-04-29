use super::{store, KvsEngine};
use crate::entry::{Entry, EntryOffset};
use crate::error::{Error, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::fs;
use std::io::Write;
use std::sync::{Arc, Mutex};

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;


#[derive(Clone)]
pub struct KvStore(Arc<Mutex<SharedKvStore>>);

#[derive(Clone)]
pub struct SharedKvStore {
    store: store::Store,
    index: HashMap<String, EntryOffset>,
    dir: PathBuf,
    curr_file_id: u32,
    uncompacted: u64,
}

impl KvStore {
    pub fn open(dir: &Path) -> Result<KvStore> {
        let _ = fs::create_dir_all(dir);
        let mut index = HashMap::new();
        let mut store = store::Store::new(dir)?;
        let curr_file_id = store.file_id;

        let uncompacted = store.load_inactive_files(&mut index)?;

        Ok(KvStore(Arc::new(Mutex::new(SharedKvStore{
            store,
            index,
            dir: dir.to_path_buf(),
            curr_file_id,
            uncompacted,
        }))))
    }
    // check existing keys in index against the corresponding file
    // copy the log entry to a new file
    // remove the inactive files from the dir as well as store hashmap
    pub fn compact(&mut self) -> Result<()> {
        // compaction output file
        let store_arc = Arc::clone(&self.0);
        let mut shared_store = store_arc.lock().unwrap();
        let compaction_file_id = shared_store.curr_file_id + 1;
        let new_filename = store::log_file_name(&shared_store.dir, compaction_file_id);
        let w = store::Writer::new(&new_filename)?;
        shared_store.store.readers.insert(compaction_file_id, store::Reader::new(&new_filename)?);

        let mut pos = 0;
        let mut writer = w.writer.lock().unwrap();
        for offset in shared_store.clone().index.values_mut() {
            let reader = shared_store.store.readers.get_mut(&offset.file_id).unwrap_or_else(|| panic!("no reader for file_id: {}", offset.file_id));
            let len = reader.read_into(offset.start, offset.end, &mut writer)?;
            *offset = EntryOffset{file_id: compaction_file_id, start: pos, end: pos + len};
            pos += len;
        }
        writer.flush()?;

        let stale_file_ids = shared_store.store.readers.keys()
            .filter(|&file_id| file_id < &compaction_file_id)
            .map(|file_id| file_id.clone())
            .collect::<Vec<_>>();

        for file_id in stale_file_ids {
            shared_store.store.readers.remove(&file_id);
            fs::remove_file(store::log_file_name(&shared_store.dir, file_id))?;
        }

        shared_store.curr_file_id = compaction_file_id + 1;
        let new_filename = store::log_file_name(&shared_store.dir, shared_store.curr_file_id);
        shared_store.store.writer = store::Writer::new(&new_filename)?;
        shared_store.store.readers.insert(compaction_file_id + 1, store::Reader::new(&new_filename)?);

        shared_store.uncompacted = 0;

        Ok(())
    }
}

impl KvsEngine for KvStore {
    fn set(&self, key: String, val: String) -> Result<()> {
        let cmd = Entry::init_set(key.clone(), val.clone());
        let serialized = serde_json::to_string(&cmd).unwrap();
        let b = serialized.as_bytes();
        let store_arc = Arc::clone(&self.0);
        let mut shared_store = store_arc.lock().unwrap();
        let pos = shared_store.store.writer.pos;

        let end_pos = shared_store.store.write(b)?;
        let curr_file_id = shared_store.curr_file_id;

        if let Some(old_val) = shared_store.index.insert(key, EntryOffset{file_id: curr_file_id, start: pos, end: end_pos}) {
            shared_store.uncompacted += old_val.end - old_val.start;
        }

        if shared_store.uncompacted > COMPACTION_THRESHOLD {
            // self.compact()?
        }

        Ok(())
    }

    fn remove(&self, key: String) -> Result<String> {
        let store_arc = Arc::clone(&self.0);
        let mut shared_store = store_arc.lock().unwrap();
        if !shared_store.index.contains_key(&key) {
            return Err(Error::DoesNotExist{key});
        }

        let cmd = Entry::init_rm(key.clone());
        let serialized = serde_json::to_string(&cmd).unwrap();
        let b = serialized.as_bytes();

        shared_store.store.write(b)?;

        if let Some(old_val) = shared_store.index.remove(&key) {
            shared_store.uncompacted += old_val.end - old_val.start;
        }

        Ok(key)
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        let store_arc = Arc::clone(&self.0);
        let mut shared_store = store_arc.lock().unwrap();
        if !shared_store.index.contains_key(&key) {
            return Ok(None);
        }
        
        let offset = shared_store.index[&key].clone();
        Ok(shared_store.store.read(offset.file_id, offset.start, offset.end)?)
    }
}
