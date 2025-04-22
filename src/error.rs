use failure::Fail;
use std::{io, string::FromUtf8Error};

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),

    #[fail(display = "{}", _0)]
    Serde(#[cause] serde_json::Error),

    #[fail(display = "key: {} does not exist", key)]
    DoesNotExist {
        key: String
    },

    #[fail(display = "{}", _0)]
    UnhandledError(String),

    #[fail(display = "sled error: {}", _0)]
    Sled(#[cause] sled::Error),

    #[fail(display = "UTF-8 error: {}", _0)]
    Utf8(#[cause] FromUtf8Error),
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Error {
        Error::Serde(err)
    }
}

impl From<sled::Error> for Error {
    fn from(err: sled::Error) -> Error {
        Error::Sled(err)
    }
}

impl From<FromUtf8Error> for Error {
    fn from(err: FromUtf8Error) -> Error {
        Error::Utf8(err)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
