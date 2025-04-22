use crate::{Result, Error, KvsEngine};
use crate::resource::{Request, Response};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::io::{Write, BufReader};
use serde_json::Deserializer;

pub struct KvsServer<E: KvsEngine> {
    engine: E,
}

impl <E: KvsEngine> KvsServer<E> {
    pub fn new(engine: E) -> Self {
        KvsServer {
            engine,
        }
    }

    pub fn run(&mut self, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => self.handle_client(stream),
                Err(err) => Err(Error::UnhandledError(format!("Connection failed: {}", err))),
            }?
        }
        Ok(())
    }

    pub fn handle_client(&mut self, stream: TcpStream) -> Result<()> {
        let reader = BufReader::new(stream.try_clone()?);
        let mut writer = stream;
        let request_reader = Deserializer::from_reader(reader).into_iter::<Request>();

        for req in request_reader {
            let req = req?;
            match req {
                Request::Get{key} => {
                    let resp: Response = match self.engine.get(key.clone()) {
                        Ok(val) => Response::Ok(val),
                        Err(_) => Response::Err(Error::DoesNotExist{key}.to_string()),
                    };
                    let b = serde_json::to_string(&resp).unwrap();
                    writer.write(b.as_bytes())?;
                    writer.flush()?;
                },
                Request::Set{key, val} => {
                    let resp: Response = match self.engine.set(key, val) {
                        Ok(()) => Response::Ok(None),
                        Err(err) => Response::Err(err.to_string()),
                    };
                    let b = serde_json::to_string(&resp).unwrap();
                    writer.write(b.as_bytes())?;
                    writer.flush()?;
                },
                Request::Rm{key} => {
                    let resp: Response = match self.engine.remove(key) {
                        Ok(key) => Response::Ok(Some(key)),
                        Err(err) => Response::Err(err.to_string()),
                    };
                    let b = serde_json::to_string(&resp).unwrap();
                    writer.write(b.as_bytes())?;
                    writer.flush()?;
                },
            }
        }
        Ok(())
    }
}
