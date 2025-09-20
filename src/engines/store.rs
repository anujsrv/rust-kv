use crate::error::{Error, Result};
use crate::entry::{Entry, EntryOffset};
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::fs;
use std::io::{copy, BufWriter, Write, BufReader, Read, Seek, SeekFrom, Take};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use serde_json::Deserializer;
use serde::{Serialize, de::DeserializeOwned};
use std::fmt::Debug;
use crossbeam_skiplist::SkipMap;
use std::marker::PhantomData;

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

// holds the readers and writers impls for the log store
pub struct Store<K, V>
where
    K: Clone + Serialize + DeserializeOwned + Ord + Send + Sync + 'static + Debug,
    V: Clone + Serialize + DeserializeOwned + Send + 'static,
{
    pub dir: Arc<PathBuf>,
    pub readers: RefCell<HashMap<u32, Reader>>,
    pub writer: Arc<Mutex<Writer>>,
    pub index: Arc<SkipMap<K, EntryOffset>>,
    pub last_compaction_point: Arc<AtomicU32>,
    _phantom: PhantomData<V>,
}

// basic wrapper over buffered writer functionality
pub struct Writer {
    pub file_id: u32,
    pub writer: BufWriter<fs::File>,
    pub pos: u64,
    pub uncompacted: u64,
}

// basic wrapper over buffered reader functionality
// additionally, encapsulates a few common read operations
pub struct Reader {
    pub reader: BufReader<fs::File>,
}

pub fn log_file_name(dir: &Path,file_id: u32) -> PathBuf {
    dir.join(format!("{}.log", file_id))
}

impl<K, V> Store<K, V>
where
    K: Clone + Serialize + DeserializeOwned + Ord + Send + Sync + 'static + Debug,
    V: Clone + Serialize + DeserializeOwned + Send + 'static,
{
    pub fn new(dir: &Path) -> Result<Store<K, V>> {
        let _ = fs::create_dir_all(dir);
        let inactive_file_ids = get_inactive_file_ids(dir)?;
        let index = SkipMap::new();
        let mut readers = HashMap::new();
        let mut new_file_id = 1;
        if let Some(file_id) = inactive_file_ids.last() {
            new_file_id = file_id + 1;
        }
        let new_filename = log_file_name(dir, new_file_id);
        let writer = Arc::new(Mutex::new(Writer::new(new_file_id, &new_filename)?));
        readers.insert(new_file_id, Reader::new(&new_filename)?);

        let store = Store{
            dir: Arc::new(dir.to_path_buf()),
            readers: RefCell::new(readers),
            writer,
            index: Arc::new(index),
            last_compaction_point: Arc::new(AtomicU32::new(0)),
            _phantom: PhantomData,
        };
        store.writer.lock().unwrap().uncompacted = store.load_inactive_files(Arc::clone(&store.index))?;

        Ok(store)
    }

    // loads older inactive log files into the given index and adds the corresponding reader to
    // internal map
    pub fn load_inactive_files(&self, index: Arc<SkipMap<K, EntryOffset>>) -> Result<u64> {
        let inactive_file_ids = get_inactive_file_ids(&self.dir)?;
        let mut uncompacted = 0;
        for file_id in inactive_file_ids {
            let filename = log_file_name(&self.dir, file_id);
            let mut reader = Reader::new(&filename)?;
            uncompacted += reader.load_index::<K, V>(file_id, Arc::clone(&index))?;
            self.readers.borrow_mut().insert(file_id, reader);
        }

        Ok(uncompacted)
    }

    pub fn read(&self, file_id: u32, start: u64, end: u64) -> Result<Option<V>> {
        self.close_stale_fds()?;
        let mut readers = self.readers.borrow_mut();
        if !readers.contains_key(&file_id) {
            let filename = log_file_name(&self.dir, file_id);
            readers.insert(file_id, Reader::new(&filename)?);
        }
        let reader = readers.get_mut(&file_id).unwrap();
        reader.read::<K, V>(start, end)
    }

    pub fn write(&self, key: K, b: &[u8]) -> Result<()> {
        let mut writer = self.writer.lock().unwrap();
        let pos = writer.pos;
        let end_pos = writer.write(b)?;
        let curr_file_id = writer.file_id;

        if let Some(old_val) = self.index.get(&key) {
            writer.uncompacted += old_val.value().end - old_val.value().start;
        }
        self.index.insert(key, EntryOffset{file_id: curr_file_id, start: pos, end: end_pos});

        if writer.uncompacted > COMPACTION_THRESHOLD {
            let new_file_id = writer.compact::<K, V>(curr_file_id, self.dir.to_path_buf(), &self.readers, Arc::clone(&self.index))?;
            self.last_compaction_point.store(new_file_id, Ordering::SeqCst);
            self.close_stale_fds()?;
        }

        Ok(())
    }

    pub fn remove(&self, key: K) -> Result<()> {
        if !self.index.contains_key(&key) {
            return Err(Error::DoesNotExist{key: format!("{:?}", key)});
        }

        let cmd: Entry<K, V> = Entry::init_rm(key.clone());
        let serialized = serde_json::to_string(&cmd).unwrap();
        let b = serialized.as_bytes();

        if let Some(old_val) = self.index.remove(&key) {
            self.writer.lock().unwrap().uncompacted += old_val.value().end - old_val.value().start;
        }
        self.write(key.clone(), b)?;

        Ok(())
    }

    pub fn close_stale_fds(&self) -> Result<()> {
        let last_compaction_point = self.last_compaction_point.load(Ordering::SeqCst);
        let mut readers = self.readers.borrow_mut();
        let stale_file_ids = readers.keys()
            .filter(|&file_id| file_id < &last_compaction_point)
            .map(|file_id| file_id.clone())
            .collect::<Vec<_>>();

        for file_id in stale_file_ids {
            readers.remove(&file_id);
            fs::remove_file(log_file_name(&self.dir, file_id))?;
        }

        Ok(())
    }
}

impl<K, V> Clone for Store<K, V>
where
    K: Clone + Serialize + DeserializeOwned + Ord + Send + Sync + 'static + Debug,
    V: Clone + Serialize + DeserializeOwned + Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            dir: self.dir.clone(),
            readers: RefCell::new(HashMap::new()),
            writer: self.writer.clone(),
            index: self.index.clone(),
            last_compaction_point: Arc::clone(&self.last_compaction_point),
            _phantom: PhantomData,
        }
    }
}

pub fn init_writer(file: &Path) -> Result<BufWriter<fs::File>> {
    Ok(BufWriter::new(
        fs::OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(&file)?,
    ))
}

impl Writer {
    pub fn new(file_id: u32, file: &Path) -> Result<Writer> {
        let writer = init_writer(file)?;
        
        Ok(Writer{
            file_id,
            pos: 0,
            uncompacted: 0,
            writer,
        })
    }

    // writes the given bytes to the file and returns the new cursor position
    pub fn write(&mut self, b: &[u8]) -> Result<u64> {
        self.writer.write(b)?;
        self.writer.flush()?;
        self.pos += b.len() as u64;

        Ok(self.pos)
    }

    // check existing keys in index against the corresponding file
    // copy the log entry to a new file
    // remove the inactive files from the dir as well as store hashmap
    pub fn compact<K2, V2>(&mut self, file_id: u32, dir: PathBuf, readers: &RefCell<HashMap<u32, Reader>>, index: Arc<SkipMap<K2, EntryOffset>>) -> Result<u32>
    where
        K2: Clone + Serialize + DeserializeOwned + Ord + Send + 'static + Debug,
        V2: Clone + Serialize + DeserializeOwned + Send + 'static,
    {
        // compaction output file
        let compaction_file_id = file_id + 1;
        let new_filename = log_file_name(&dir, compaction_file_id);
        let w = Writer::new(compaction_file_id, &new_filename)?;
        let mut readers_mut = readers.borrow_mut();
        readers_mut.insert(compaction_file_id, Reader::new(&new_filename)?);

        let mut pos = 0;
        let mut writer = w.writer;
        for entry in index.iter() {
            let offset: &EntryOffset = entry.value();
            let reader = readers_mut.get_mut(&offset.file_id).unwrap_or_else(|| panic!("no reader for file_id: {}", offset.file_id));
            let len = reader.read_into(offset.start, offset.end, &mut writer)?;

            index.insert(entry.key().clone(), EntryOffset{file_id: compaction_file_id, start: pos, end: pos + len});
            pos += len;
        }
        writer.flush()?;

        let new_filename = log_file_name(&dir, compaction_file_id + 1);
        self.writer = init_writer(&new_filename)?;
        self.pos = 0;
        self.uncompacted = 0;
        readers_mut.insert(compaction_file_id + 1, Reader::new(&new_filename)?);

        Ok(compaction_file_id)
    }
}

impl Reader {
    pub fn new(file: &Path) -> Result<Reader> {
        let f = fs::File::open(&file)?;
        let reader = BufReader::new(f.try_clone()?);

        Ok(Reader{
            reader,
        })
    }

    // internal function for reading limited number of bytes from given offset
    fn read_limited(&mut self, start: u64, end: u64) -> Result<Take<&mut BufReader<fs::File>>> {
        let reader = &mut self.reader;
        reader.seek(SeekFrom::Start(start))?;
        Ok(reader.take(end - start))
    }

    // reads from the given offset and returns a value if Set command is present at the
    // offset, otherwise returns None
    pub fn read<K, V>(&mut self, start: u64, end: u64) -> Result<Option<V>>
    where
        K: Clone + Serialize + DeserializeOwned + Ord + Send + Sync + 'static + Debug,
        V: Clone + Serialize + DeserializeOwned + Send + 'static,
    {
        let reader = self.read_limited(start, end)?;

        if let Entry::Set{val, ..} = serde_json::from_reader::<_, Entry<K, V>>(reader)? {
            Ok(Some(val))
        } else {
            Ok(None)
        }
    }

    // reads from the given offset and copies to the given writer instance.
    pub fn read_into(&mut self, start: u64, end: u64, writer: &mut BufWriter<fs::File>) -> Result<u64> {
        let mut reader = self.read_limited(start, end)?;
        Ok(copy(&mut reader, writer)?)
    }

    // loads index from the corresponding log file and computes and returns the size of uncompacted bytes
    pub fn load_index<K, V>(&mut self, file_id: u32, index: Arc<SkipMap<K, EntryOffset>>) -> Result<u64>
    where
        K: Clone + Serialize + DeserializeOwned + Ord + Send + Sync + 'static + Debug,
        V: Clone + Serialize + DeserializeOwned + Send + 'static,
    {
        let reader = &mut self.reader;
        let mut cmd_start = reader.seek(SeekFrom::Start(0))?;
        let mut stream = Deserializer::from_reader(reader).into_iter::<Entry<K, V>>();
        let mut uncompacted = 0;

        while let Some(cmd) = stream.next() {
            let cmd_end = stream.byte_offset() as u64;
            match cmd? {
                Entry::Set {key, ..} => {
                    if let Some(old_val) = index.get(&key) {
                        uncompacted += old_val.value().end - old_val.value().start;
                    }
                    index.insert(key, EntryOffset{file_id, start: cmd_start, end: cmd_end});
                },
                Entry::Rm {key} => {
                    if let Some(old_val) = index.remove(&key) {
                        uncompacted += old_val.value().end - old_val.value().start;
                    }
                    uncompacted += cmd_end - cmd_start;
                }
            };
            cmd_start = cmd_end;
        }

        Ok(uncompacted)
    }
}


// goes through the log directory and returns all old/inactive file ids in a sorted order.
fn get_inactive_file_ids(dir: &Path) -> Result<Vec<u32>> {
   let filenames = fs::read_dir(dir)?
       .filter_map(|res| res.ok())
       .map(|entry| entry.path())
       .filter_map(|path| {
           if path.is_file() && path.extension().map_or(false, |ext| ext == "log") {
               Some(path)
           } else {
               None
           }
       })
       .collect::<Vec<_>>();

    let mut file_ids: Vec<u32> = Vec::new();
    for filepath in filenames {
        let filename = filepath.file_name().unwrap();
        let filename_str = filename.to_str().unwrap();
        let file_id = filename_str.split(".log").collect::<Vec<_>>()[0];
        file_ids.push(file_id.parse::<u32>().unwrap());
    }

    file_ids.sort();

    Ok(file_ids)
}
