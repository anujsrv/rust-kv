use crate::{Error, Result};
use crate::resource::{Request, Response};
use std::io::{BufReader, Write};
use serde::Deserialize;
use serde_json::de::{Deserializer, IoRead};
use std::net::{SocketAddr, TcpStream};

pub struct KvsClient {
    request_stream: TcpStream,
    response_stream: Deserializer<IoRead<BufReader<TcpStream>>>,
}

impl KvsClient {
    pub fn connect(addr: SocketAddr) -> Result<KvsClient> {
        let request_stream = TcpStream::connect(addr)?;
        let response_stream = Deserializer::from_reader(BufReader::new(request_stream.try_clone()?));
        Ok(KvsClient{
            request_stream,
            response_stream,
        })
    }
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let payload = serde_json::to_string(&Request::Get{key})?;
        let b = payload.as_bytes();
        self.request_stream.write_all(b)?;
        self.request_stream.flush()?;
        let response = Response::deserialize(&mut self.response_stream)?;
        match response {
            Response::Ok(val) => Ok(val),
            Response::Err(err) => Err(Error::UnhandledError(err)),
        }
    }
    pub fn remove(&mut self, key: String) -> Result<()> {
        let payload = serde_json::to_string(&Request::Rm{key})?;
        let b = payload.as_bytes();
        self.request_stream.write_all(b)?;
        self.request_stream.flush()?;
        let response = Response::deserialize(&mut self.response_stream)?;
        match response {
            Response::Ok(_) => Ok(()),
            Response::Err(err) => Err(Error::UnhandledError(err)),
        }
    }
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let payload = serde_json::to_string(&Request::Set{key, val: value})?;
        let b = payload.as_bytes();
        self.request_stream.write_all(b)?;
        self.request_stream.flush()?;
        let response = Response::deserialize(&mut self.response_stream)?;
        match response {
            Response::Ok(_) => Ok(()),
            Response::Err(err) => Err(Error::UnhandledError(err)),
        }
    }
}
