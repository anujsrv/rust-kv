use crate::{Result, Entry, EntryOffset};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::fs;
use std::io::{copy, BufWriter, Write, BufReader, Read, Seek, SeekFrom, Take};
use serde_json::Deserializer;

pub struct Store {
    pub dir: PathBuf,
    pub file_id: u32,
    pub readers: HashMap<u32, Reader>,
    pub writer: Writer,
}

pub struct Writer {
    pub writer: BufWriter<fs::File>,
    pub pos: u64,
}

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
            writer,
        })
    }

    pub fn write(&mut self, b: &[u8]) -> Result<u64> {
        self.writer.write(b)?;
        self.writer.flush()?;
        self.pos += b.len() as u64;

        Ok(self.pos)
    }
}

impl Reader {
    pub fn new(file: &Path) -> Result<Reader> {
        let f = fs::File::open(&file)?;
        let reader = BufReader::new(f);

        Ok(Reader{
            reader,
        })
    }

    fn limit_reader(&mut self, start: u64, end: u64) -> Result<Take<&mut BufReader<fs::File>>> {
        let reader = &mut self.reader;
        reader.seek(SeekFrom::Start(start))?;
        Ok(reader.take(end - start))
    }

    pub fn read(&mut self, start: u64, end: u64) -> Result<Option<String>> {
        let reader = self.limit_reader(start, end)?;

        if let Entry::Set{val, ..} = serde_json::from_reader(reader)? {
            Ok(Some(val))
        } else {
            Ok(None)
        }
    }

    pub fn read_into(&mut self, start: u64, end: u64, writer: &mut BufWriter<fs::File>) -> Result<u64> {
        let mut reader = self.limit_reader(start, end)?;
        Ok(copy(&mut reader, writer)?)
    }

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
