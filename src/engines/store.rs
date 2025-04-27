use crate::error::Result;
use crate::entry::{Entry, EntryOffset};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::fs;
use std::io::{copy, BufWriter, Write, BufReader, Read, Seek, SeekFrom, Take};
use std::sync::{Arc, Mutex};
use serde_json::Deserializer;

// holds the readers and writers impls for the log store
pub struct Store {
    pub dir: PathBuf,
    pub file_id: u32,
    pub readers: HashMap<u32, Reader>,
    pub writer: Writer,
}

// basic wrapper over buffered writer functionality
pub struct Writer {
    pub writer: Arc<Mutex<BufWriter<fs::File>>>,
    pub pos: u64,
}

// basic wrapper over buffered reader functionality
// additionally, encapsulates a few common read operations
pub struct Reader {
    pub reader: BufReader<fs::File>,
}

pub fn log_file_name(dir: &Path,file_id: u32) -> PathBuf {
    dir.join(format!("{}.log", file_id))
}

impl Store {
    pub fn new(dir: &Path) -> Result<Store> {
        let _ = fs::create_dir_all(dir);
        let inactive_file_ids = get_inactive_file_ids(dir)?;
        let mut readers = HashMap::new();
        let mut new_file_id = 0;
        if let Some(file_id) = inactive_file_ids.last() {
            new_file_id = file_id + 1;
        }
        let new_filename = log_file_name(dir, new_file_id);
        let writer = Writer::new(&new_filename)?;
        readers.insert(new_file_id, Reader::new(&new_filename)?);

        Ok(Store{
            dir: dir.to_path_buf(),
            file_id: new_file_id,
            readers,
            writer,
        })
    }

    // loads older inactive log files into the given index and adds the corresponding reader to
    // internal map
    pub fn load_inactive_files(&mut self, index: &mut HashMap<String, EntryOffset>) -> Result<u64> {
        let inactive_file_ids = get_inactive_file_ids(&self.dir)?;
        let mut uncompacted = 0;
        for file_id in inactive_file_ids {
            let filename = self.dir.join(format!("{}.log", file_id));
            let mut reader = Reader::new(&filename)?;
            uncompacted += reader.load_index(file_id, index)?;
            self.readers.insert(file_id, reader);
        }

        Ok(uncompacted)
    }

    pub fn read(&mut self, file_id: u32, start: u64, end: u64) -> Result<Option<String>> {
        let reader = self.readers.get_mut(&file_id).unwrap();
        reader.read(start, end)
    }

    pub fn write(&mut self, b: &[u8]) -> Result<u64> {
        self.writer.write(b)
    }

}

impl Clone for Store {
    fn clone(&self) -> Self {
        Self { dir: self.dir.clone(), file_id: self.file_id.clone(), readers: self.readers.clone(), writer: self.writer.clone() }
    }
}

impl Writer {
    pub fn new(file: &Path) -> Result<Writer> {
        let writer = BufWriter::new(
            fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&file)?,
        );
        
        Ok(Writer{
            pos: 0,
            writer: Arc::new(Mutex::new(writer)),
        })
    }

    // writes the given bytes to the file and returns the new cursor position
    pub fn write(&mut self, b: &[u8]) -> Result<u64> {
        let mut writer = self.writer.lock().unwrap();
        writer.write(b)?;
        writer.flush()?;
        self.pos += b.len() as u64;

        Ok(self.pos)
    }
}

impl Clone for Writer {
    fn clone(&self) -> Self {
        Self { writer: self.writer.clone(), pos: self.pos.clone() }
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

    // reads from the given offset and returns a string value if Set command is present at the
    // offset, otherwise returns None
    pub fn read(&mut self, start: u64, end: u64) -> Result<Option<String>> {
        let reader = self.read_limited(start, end)?;

        if let Entry::Set{val, ..} = serde_json::from_reader(reader)? {
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
    pub fn load_index(&mut self, file_id: u32, index: &mut HashMap::<String, EntryOffset>) -> Result<u64> {
        let reader = &mut self.reader;
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
}

impl Clone for Reader {
    fn clone(&self) -> Self {
        let cloned_file = self.reader.get_ref().try_clone().unwrap();
        Self { reader: BufReader::new(cloned_file) }
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
