use crate::{Result, Error, KvsEngine, ThreadPool};
use crate::resource::{Request, Response};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::io::{Write, BufReader};
use serde_json::Deserializer;

pub struct KvsServer<E: KvsEngine> {
    engine: E,
    pool: ThreadPool,
}

impl <E: KvsEngine> KvsServer<E> {
    pub fn new(engine: E, pool: ThreadPool) -> Self {
        KvsServer {
            engine,
            pool,
        }
    }

    pub fn run(&self, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            let stream = stream.unwrap();
            let engine = self.engine.clone();
            self.pool.execute(move || {
               handle_client(engine, stream).unwrap();
            });
        }
        Ok(())
    }
}

fn handle_client<E: KvsEngine>(engine: E, stream: TcpStream) -> Result<()> {
    let reader = BufReader::new(stream.try_clone()?);
    let mut writer = stream;
    let request_reader = Deserializer::from_reader(reader).into_iter::<Request>();

    for req in request_reader {
        let req = req?;
        match req {
            Request::Get{key} => {
                let resp: Response = match engine.get(key.clone()) {
                    Ok(val) => Response::Ok(val),
                    Err(_) => Response::Err(Error::DoesNotExist{key}.to_string()),
                };
                let b = serde_json::to_string(&resp).unwrap();
                writer.write(b.as_bytes())?;
                writer.flush()?;
            },
            Request::Set{key, val} => {
                let resp: Response = match engine.set(key, val) {
                    Ok(()) => Response::Ok(None),
                    Err(err) => Response::Err(err.to_string()),
                };
                let b = serde_json::to_string(&resp).unwrap();
                writer.write(b.as_bytes())?;
                writer.flush()?;
            },
            Request::Rm{key} => {
                let resp: Response = match engine.remove(key) {
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
