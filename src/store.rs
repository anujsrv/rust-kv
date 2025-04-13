use crate::{Error, Result};
use std::collections::{HashMap};
use std::path::Path;
use std::fs;
use std::io::{BufWriter, Write, BufReader, Read, Seek, SeekFrom};
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set {key: String, val: String},
    Rm {key: String},
}

impl Command {
    pub fn init_set(key: String, val: String) -> Command {
        Command::Set{
            key,
            val,
        }
    }

    pub fn init_rm(key: String) -> Command {
        Command::Rm{
            key,
        }
    }
}

pub struct KvStore {
    index: HashMap<String, String>,
    writer: BufWriter<fs::File>,
}

impl KvStore {
    pub fn open(dir: &Path) -> Result<KvStore> {
        let _ = fs::create_dir_all(dir);
        let filename = dir.join("wal.log");
        let index = load_index(&filename)?;

        let writer = BufWriter::new(
            fs::OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(filename)?,
        );

        Ok(KvStore{writer, index})
    }

    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        let cmd = Command::init_set(key.clone(), val.clone());
        let serialized = serde_json::to_string(&cmd).unwrap();

        self.index.insert(key, val);

        self.writer.write(serialized.as_bytes())?;
        self.writer.flush()?;

        Ok(())
    }

    pub fn remove(&mut self, key: String) -> Result<String> {
        if !self.index.contains_key(&key) {
            return Err(Error::DoesNotExist{key});
        }

        let cmd = Command::init_rm(key.clone());
        let serialized = serde_json::to_string(&cmd).unwrap();

        self.index.remove(&key);

        self.writer.write(serialized.as_bytes())?;
        self.writer.flush()?;

        Ok(key)
    }

    pub fn get(&self, key: String) -> Result<Option<String>> {
        if !self.index.contains_key(&key) {
            return Ok(None);
        }
        
        Ok(Some(self.index[&key].clone()))
    }
}

fn load_index(file: &Path) -> Result<HashMap<String, String>> {
    let mut index = HashMap::new();
    let f = match fs::File::open(file) {
        Ok(f) => f,
        Err(_) => return Ok(index),
    };
    let mut reader = BufReader::new(f);

    let _  = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();

    while let Some(cmd) = stream.next() {
        match cmd? {
            Command::Set {key, val} => index.insert(key, val),
            Command::Rm {key} => index.remove(&key),
        };
    }

    Ok(index)
}
