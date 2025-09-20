use crate::{Result, KvsEngine, ThreadPool};
use crate::resource::{Request, Response};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::io::{Write, BufReader};
use serde_json::Deserializer;
use serde::{Serialize, de::DeserializeOwned};
use std::fmt::Debug;
use std::marker::PhantomData;

pub struct KvsServer<K, V, E: KvsEngine<K, V>>
where
    K: Clone + Serialize + DeserializeOwned + Ord + Send + Sync + 'static + Debug,
    V: Clone + Serialize + DeserializeOwned + Send + 'static,
    E: KvsEngine<K, V>,
{
    engine: E,
    pool: ThreadPool,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V, E> KvsServer<K, V, E>
where
    K: Clone + Serialize + DeserializeOwned + Ord + Send + Sync + 'static + Debug,
    V: Clone + Serialize + DeserializeOwned + Send + 'static,
    E: KvsEngine<K, V>,
{
    pub fn new(engine: E, pool: ThreadPool) -> Self {
        KvsServer {
            engine,
            pool,
            _phantom: PhantomData,
        }
    }

    pub fn run(&self, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            let stream = stream.unwrap();
            let engine = self.engine.clone();
            self.pool.execute(move || {
               handle_client::<K, V, E>(engine, stream).unwrap();
            });
        }
        Ok(())
    }
}

fn handle_client<K, V, E>(engine: E, stream: TcpStream) -> Result<()>
where
    K: Clone + Serialize + DeserializeOwned + Ord + Send + Sync + 'static + Debug,
    V: Clone + Serialize + DeserializeOwned + Send + 'static,
    E: KvsEngine<K, V>,
{
    let reader = BufReader::new(stream.try_clone()?);
    let mut writer = stream;
    let request_reader = Deserializer::from_reader(reader).into_iter::<Request<K, V>>();

    for req in request_reader {
        let req = req?;
        match req {
            Request::Get{key} => {
                let resp: Response<V> = match engine.get(key) {
                    Ok(val) => Response::<V>::Ok(val),
                    Err(err) => Response::<V>::Err(err.to_string()),
                };
                let b = serde_json::to_string(&resp).unwrap();
                writer.write_all(b.as_bytes())?;
                writer.flush()?;
            },
            Request::Set{key, val} => {
                let resp: Response<V> = match engine.set(key, val) {
                    Ok(()) => Response::<V>::Ok(None),
                    Err(err) => Response::<V>::Err(err.to_string()),
                };
                let b = serde_json::to_string(&resp).unwrap();
                writer.write_all(b.as_bytes())?;
                writer.flush()?;
            },
            Request::Rm{key} => {
                let resp: Response<V> = match engine.remove(key.clone()) {
                    Ok(_) => Response::<V>::Ok(None),
                    Err(err) => Response::<V>::Err(err.to_string()),
                };
                let b = serde_json::to_string(&resp).unwrap();
                writer.write_all(b.as_bytes())?;
                writer.flush()?;
            },
        }
    }
    Ok(())
}
