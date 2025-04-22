use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Get {key: String},
    Set {key: String, val: String},
    Rm {key: String},
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Ok(Option<String>),
    Err(String),
}
