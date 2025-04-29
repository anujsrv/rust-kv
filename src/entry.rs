use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum Entry {
    Set {key: String, val: String},
    Rm {key: String},
}

#[derive(Clone, Debug)]
pub struct EntryOffset {
    pub file_id: u32,
    pub start: u64,
    pub end: u64,
}

impl Entry {
    pub fn init_set(key: String, val: String) -> Entry {
        Entry::Set{
            key,
            val,
        }
    }

    pub fn init_rm(key: String) -> Entry {
        Entry::Rm{
            key,
        }
    }
}
