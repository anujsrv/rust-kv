use clap::{Parser, ValueEnum};
use kvs::*;
use log::LevelFilter;
use log::{error, info, warn};
use simple_logger::SimpleLogger;
use std::env::current_dir;
use std::fs;
use std::net::SocketAddr;
use std::process::exit;
use std::str::FromStr;

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:4000";
const DEFAULT_ENGINE: Engine = Engine::kvs;

#[derive(Parser, Debug)]
#[command(name="kvs-server", version)]
struct Opt {
    #[arg(
        long,
        help = "Sets the listening address",
        value_name = "IP:PORT",
        default_value(DEFAULT_LISTENING_ADDRESS),
    )]
    addr: SocketAddr,
    #[arg(
        long,
        value_enum,
        help = "Sets the storage engine",
    )]
    engine: Option<Engine>,
}

#[allow(non_camel_case_types)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum Engine {
    kvs,
    sled,
}

impl FromStr for Engine {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "kvs" => Ok(Engine::kvs),
            "sled" => Ok(Engine::sled),
            _ => Err(Error::UnhandledError(format!("Invalid engine type: {}", s))),
        }
    }
}

fn main() {
    SimpleLogger::new().with_level(LevelFilter::Info).init().unwrap();
    let mut opt = Opt::parse();
    let res = current_engine().and_then(move |curr_engine| {
        if opt.engine.is_none() {
            opt.engine = curr_engine;
        }
        if curr_engine.is_some() && opt.engine != curr_engine {
            error!("Wrong engine!");
            exit(1);
        }
        run(opt)
    });
    if let Err(e) = res {
        error!("{}", e);
        exit(1);
    }
}

fn run(opt: Opt) -> Result<()> {
    let engine = opt.engine.unwrap_or(DEFAULT_ENGINE);
    info!("kvs-server {}", env!("CARGO_PKG_VERSION"));
    info!("Storage engine: {:?}", engine);
    info!("Listening on {}", opt.addr);

    // write engine to engine file
    fs::write(current_dir()?.join("engine"), format!("{:?}", engine))?;

    match engine {
        Engine::kvs => run_with_engine(KvStore::open(&current_dir()?)?, opt.addr),
        Engine::sled => run_with_engine(SledKvsEngine::new(sled::open(current_dir()?)?), opt.addr),
    }
}

fn run_with_engine<E: KvsEngine>(engine: E, addr: SocketAddr) -> Result<()> {
    let mut server = KvsServer::new(engine);
    server.run(addr)
}

fn current_engine() -> Result<Option<Engine>> {
    let engine = current_dir()?.join("engine");
    if !engine.exists() {
        return Ok(None);
    }

    match fs::read_to_string(engine)?.parse() {
        Ok(engine) => Ok(Some(engine)),
        Err(e) => {
            warn!("The content of engine file is invalid: {}", e);
            Ok(None)
        }
    }
}
