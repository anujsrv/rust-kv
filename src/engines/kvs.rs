use super::{store, KvsEngine};
use crate::entry::{Entry, EntryOffset};
use crate::error::{Error, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::fs;
use std::io::Write;

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

pub struct KvStore {
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

        Ok(KvStore{
            store,
            index,
            dir: dir.to_path_buf(),
            curr_file_id,
            uncompacted,
        })
    }
    // check existing keys in index against the corresponding file
    // copy the log entry to a new file
    // remove the inactive files from the dir as well as store hashmap
    pub fn compact(&mut self) -> Result<()> {
        // compaction output file
        let compaction_file_id = self.curr_file_id + 1;
        let new_filename = store::log_file_name(&self.dir, compaction_file_id);
        let mut w = store::Writer::new(&new_filename)?;
        self.store.readers.insert(compaction_file_id, store::Reader::new(&new_filename)?);

        let mut pos = 0;
        for offset in self.index.values_mut() {
            let reader = self.store.readers.get_mut(&offset.file_id).unwrap_or_else(|| panic!("no reader for file_id: {}", offset.file_id));
            let len = reader.read_into(offset.start, offset.end, &mut w.writer)?;
            *offset = EntryOffset{file_id: compaction_file_id, start: pos, end: pos + len};
            pos += len;
        }
        w.writer.flush()?;

        let stale_file_ids = self.store.readers.keys()
            .filter(|&file_id| file_id < &compaction_file_id)
            .map(|file_id| file_id.clone())
            .collect::<Vec<_>>();

        for file_id in stale_file_ids {
            self.store.readers.remove(&file_id);
            fs::remove_file(store::log_file_name(&self.dir, file_id))?;
        }

        self.curr_file_id = compaction_file_id + 1;
        let new_filename = store::log_file_name(&self.dir, self.curr_file_id);
        self.store.writer = store::Writer::new(&new_filename)?;
        self.store.readers.insert(self.curr_file_id, store::Reader::new(&new_filename)?);

        self.uncompacted = 0;

        Ok(())
    }
}

impl KvsEngine for KvStore {
    fn set(&mut self, key: String, val: String) -> Result<()> {
        let cmd = Entry::init_set(key.clone(), val.clone());
        let serialized = serde_json::to_string(&cmd).unwrap();
        let b = serialized.as_bytes();
        let pos = self.store.writer.pos;

        let end_pos = self.store.write(b)?;

        if let Some(old_val) = self.index.insert(key, EntryOffset{file_id: self.curr_file_id, start: pos, end: end_pos}) {
            self.uncompacted += old_val.end - old_val.start;
        }

        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?
        }

        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<String> {
        if !self.index.contains_key(&key) {
            return Err(Error::DoesNotExist{key});
        }

        let cmd = Entry::init_rm(key.clone());
        let serialized = serde_json::to_string(&cmd).unwrap();
        let b = serialized.as_bytes();

        self.store.write(b)?;

        if let Some(old_val) = self.index.remove(&key) {
            self.uncompacted += old_val.end - old_val.start;
        }

        Ok(key)
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        if !self.index.contains_key(&key) {
            return Ok(None);
        }
        
        let offset = self.index[&key].clone();
        Ok(self.store.read(offset.file_id, offset.start, offset.end)?)
    }
}
