use crate::{Error, Result};
use std::collections::HashMap;
use std::path::Path;
use std::fs;
use std::io::{BufWriter, Write, BufReader, Read, Seek, SeekFrom};
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

#[derive(Clone)]
struct CmdOffset {
    start: u64,
    end: u64,
}

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
    index: HashMap<String, CmdOffset>,
    pos: u64,
    reader: BufReader<fs::File>,
    writer: BufWriter<fs::File>,
}

impl KvStore {
    pub fn open(dir: &Path) -> Result<KvStore> {
        let _ = fs::create_dir_all(dir);
        let filename = dir.join("wal.log");
        let writer = BufWriter::new(
            fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&filename)?,
        );
        let f = fs::File::open(&filename)?;
        let mut reader = BufReader::new(f);
        let (index, pos) = load_index(&mut reader)?;

        Ok(KvStore{pos, reader, writer, index})
    }

    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        let cmd = Command::init_set(key.clone(), val.clone());
        let serialized = serde_json::to_string(&cmd).unwrap();
        let b = serialized.as_bytes();
        let end_pos = self.pos + b.len() as u64;

        self.index.insert(key, CmdOffset{start: self.pos, end: end_pos});

        self.writer.write(b)?;
        self.writer.flush()?;

        self.pos = end_pos;

        Ok(())
    }

    pub fn remove(&mut self, key: String) -> Result<String> {
        if !self.index.contains_key(&key) {
            return Err(Error::DoesNotExist{key});
        }

        let cmd = Command::init_rm(key.clone());
        let serialized = serde_json::to_string(&cmd).unwrap();
        let b = serialized.as_bytes();

        self.index.remove(&key);

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
        println!("seeking for key: {}, offset start: {}, end: {}", key, offset.start, offset.end);
        let _ = self.reader.seek(SeekFrom::Start(offset.start));
        let reader = (&mut self.reader).take(offset.end - offset.start);

        if let Command::Set{val, ..} = serde_json::from_reader(reader)? {
            Ok(Some(val))
        } else {
            Ok(None)
        }
    }
}

fn load_index(reader: &mut BufReader<fs::File>) -> Result<(HashMap<String, CmdOffset>, u64)> {
    let mut index = HashMap::new();

    let mut cmd_start = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
    let mut pos = 0;

    while let Some(cmd) = stream.next() {
        let cmd_end = stream.byte_offset() as u64;
        match cmd? {
            Command::Set {key, ..} => index.insert(key, CmdOffset{start: cmd_start, end: cmd_end}),
            Command::Rm {key} => index.remove(&key),
        };
        cmd_start = cmd_end;
        pos = cmd_end;
    }

    Ok((index, pos))
}
