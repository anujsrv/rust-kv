pub use store::KvStore;
pub use error::{Error, Result};
pub use entry::{Entry, EntryOffset};

mod error;
mod store;
mod entry;
