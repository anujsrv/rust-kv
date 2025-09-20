use serde::{Serialize, Deserialize};
use std::fmt::Debug;

#[derive(Debug, Serialize, Deserialize)]
pub enum Request<K, V> 
where
    K: Clone + Ord + Send + Sync + 'static + Debug,
    V: Clone + Send + 'static,
{
    Get {key: K},
    Set {key: K, val: V},
    Rm {key: K},
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response<V> 
where
    V: Clone + Send + 'static,
{
    Ok(Option<V>),
    Err(String),
}
