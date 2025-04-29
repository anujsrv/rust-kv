use clap::{Parser, Subcommand, value_parser};
use kvs::{KvsClient, Result};
use std::net::SocketAddr;
use std::process::exit;

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:4000";

#[derive(Parser, Debug)]
#[clap(disable_help_flag = true)]
#[command(version)]
struct Opt {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    #[command(id = "get", about = "Get the string value of a given string key")]
    Get {
        #[arg(id = "KEY", help = "A string key")]
        key: String,
        #[arg(
            long,
            help = "Sets the server address",
            default_value(DEFAULT_LISTENING_ADDRESS),
            value_parser(value_parser!(SocketAddr))
        )]
        addr: SocketAddr,
    },
    #[command(id = "set", about = "Set the value of a string key to a string")]
    Set {
        #[arg(id = "KEY", help = "A string key")]
        key: String,
        #[arg(id = "VALUE", help = "The string value of the key")]
        value: String,
        #[arg(
            long,
            help = "Sets the server address",
            default_value(DEFAULT_LISTENING_ADDRESS),
            value_parser(value_parser!(SocketAddr))
        )]
        addr: SocketAddr,
    },
    #[command(id = "rm", about = "Remove a given string key")]
    Remove {
        #[arg(id = "KEY", help = "A string key")]
        key: String,
        #[arg(
            long,
            help = "Sets the server address",
            default_value(DEFAULT_LISTENING_ADDRESS),
            value_parser(value_parser!(SocketAddr))
        )]
        addr: SocketAddr,
    },
}

fn main() {
    let opt = Opt::parse();
    if let Err(e) = run(opt) {
        eprintln!("{}", e);
        exit(1);
    }
}

fn run(opt: Opt) -> Result<()> {
    match opt.command {
        Command::Get { key, addr } => {
            let mut client = KvsClient::connect(addr)?;
            if let Some(value) = client.get(key)? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        Command::Set { key, value, addr } => {
            let mut client = KvsClient::connect(addr)?;
            client.set(key, value)?;
        }
        Command::Remove { key, addr } => {
            let mut client = KvsClient::connect(addr)?;
            client.remove(key)?;
        }
    }
    Ok(())
}
