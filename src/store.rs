use crate::{Error, Result, Entry, EntryOffset};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::fs;
use std::io::{copy, BufWriter, Write, BufReader, Read, Seek, SeekFrom};
use serde_json::Deserializer;

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

pub struct KvStore {
    dir: PathBuf,
    index: HashMap<String, EntryOffset>,
    curr_file_id: u32,
    pos: u64,
    readers: HashMap<u32, BufReader<fs::File>>,
    writer: BufWriter<fs::File>,
    uncompacted: u64,
}

impl KvStore {
    pub fn open(dir: &Path) -> Result<KvStore> {
        let _ = fs::create_dir_all(dir);
        let inactive_file_ids = get_inactive_file_ids(dir)?;
        let mut index = HashMap::new();
        let mut readers = HashMap::new();
        let mut new_file_id = 0;
        let mut uncompacted = 0;
        for file_id in inactive_file_ids {
            let filename = dir.join(format!("{}.log", file_id));
            let f = fs::File::open(&filename)?;
            let mut reader = BufReader::new(f);
            uncompacted += load_index(file_id, &mut reader, &mut index)?;
            readers.insert(file_id, reader);
            new_file_id = file_id + 1;
        }

        let new_filename = dir.join(format!("{}.log", new_file_id));
        let pos = 0;
        let writer = BufWriter::new(
            fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&new_filename)?,
        );
        let f = fs::File::open(&new_filename)?;
        readers.insert(new_file_id, BufReader::new(f));

        Ok(KvStore{
            dir: dir.to_path_buf(),
            pos,
            readers,
            writer,
            index,
            curr_file_id: new_file_id,
            uncompacted,
        })
    }

    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        let cmd = Entry::init_set(key.clone(), val.clone());
        let serialized = serde_json::to_string(&cmd).unwrap();
        let b = serialized.as_bytes();
        let end_pos = self.pos + b.len() as u64;

        if let Some(old_val) = self.index.insert(key, EntryOffset{file_id: self.curr_file_id, start: self.pos, end: end_pos}) {
            self.uncompacted += old_val.end - old_val.start;
        }

        self.writer.write(b)?;
        self.writer.flush()?;

        self.pos = end_pos;

        // println!("uncompacted: {}", self.uncompacted);
        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?
        }

        Ok(())
    }

    pub fn remove(&mut self, key: String) -> Result<String> {
        if !self.index.contains_key(&key) {
            return Err(Error::DoesNotExist{key});
        }

        let cmd = Entry::init_rm(key.clone());
        let serialized = serde_json::to_string(&cmd).unwrap();
        let b = serialized.as_bytes();

        if let Some(old_val) = self.index.remove(&key) {
            self.uncompacted += old_val.end - old_val.start;
        }

        self.writer.write(b)?;
        self.writer.flush()?;

        self.pos += b.len() as u64;

        Ok(key)
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if !self.index.contains_key(&key) {
            return Ok(None);
        }
        
        let offset = self.index[&key].clone();
        let reader = self.readers.get_mut(&offset.file_id).unwrap();
        let _ = reader.seek(SeekFrom::Start(offset.start));
        let limit_reader = reader.take(offset.end - offset.start);

        if let Entry::Set{val, ..} = serde_json::from_reader(limit_reader)? {
            Ok(Some(val))
        } else {
            Ok(None)
        }
    }

    // check existing keys in index against the corresponding file
    // copy the log entry to a new file
    // remove the inactive files from the dir as well as store hashmap
    pub fn compact(&mut self) -> Result<()> {
        // compaction output file
        let compaction_file_id = self.curr_file_id + 1;
        let new_filename = self.dir.join(format!("{}.log", compaction_file_id));
        let mut writer = BufWriter::new(
            fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&new_filename)?,
        );
        let f = fs::File::open(&new_filename)?;
        self.readers.insert(compaction_file_id, BufReader::new(f));

        println!("compaction_file_id: {}", compaction_file_id);

        let mut pos = 0;
        for offset in self.index.values_mut() {
            let reader = self.readers.get_mut(&offset.file_id).unwrap_or_else(|| panic!("no reader for file_id: {}", offset.file_id));
            let _ = reader.seek(SeekFrom::Start(offset.start));
            let mut limit_reader = reader.take(offset.end - offset.start);
            let len = copy(&mut limit_reader, &mut writer)?;
            *offset = EntryOffset{file_id: compaction_file_id, start: pos, end: pos + len};
            pos += len;
        }
        self.writer.flush()?;

        let stale_file_ids = self.readers.keys()
            .filter(|&file_id| file_id < &compaction_file_id)
            .map(|file_id| file_id.clone())
            .collect::<Vec<_>>();

        for file_id in stale_file_ids {
            self.readers.remove(&file_id);
            fs::remove_file(self.dir.join(format!("{}.log", file_id)))?;
        }

        self.curr_file_id = compaction_file_id + 1;
        let new_filename = self.dir.join(format!("{}.log", self.curr_file_id));
        self.writer = BufWriter::new(
            fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&new_filename)?,
        );
        let f = fs::File::open(&new_filename)?;
        self.readers.insert(self.curr_file_id, BufReader::new(f));

        self.pos = 0;
        self.uncompacted = 0;

        Ok(())
    }
}

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

fn load_index(file_id: u32, reader: &mut BufReader<fs::File>, index: &mut HashMap::<String, EntryOffset>) -> Result<u64> {
    let mut cmd_start = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Entry>();
    let mut uncompacted = 0;

    while let Some(cmd) = stream.next() {
        let cmd_end = stream.byte_offset() as u64;
        match cmd? {
            Entry::Set {key, ..} => {
                if let Some(old_val) = index.insert(key, EntryOffset{file_id, start: cmd_start, end: cmd_end}) {
                    uncompacted += old_val.end - old_val.start;
                }
            },
            Entry::Rm {key} => {
                if let Some(old_val) = index.remove(&key) {
                    uncompacted += old_val.end - old_val.start;
                }
                uncompacted += cmd_end - cmd_start;
            }
        };
        cmd_start = cmd_end;
    }

    Ok(uncompacted)
}
