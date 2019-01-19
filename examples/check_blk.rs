/// Benchmark regular indexing flow (using JSONRPC), don't persist the resulting index.
extern crate electrs;
extern crate error_chain;

#[macro_use]
extern crate log;

use std::io;
use std::io::Read;

use electrs::{bulk::parse_blocks, config::Config, errors::*};
use error_chain::ChainedError;

fn run() -> Result<()> {
    let config = Config::from_args();
    let blob = io::stdin()
        .bytes()
        .map(|b| b.expect("failed to read from stdin"))
        .collect();
    parse_blocks(blob, config.network_type.magic())?;
    Ok(())
}

fn main() {
    if let Err(e) = run() {
        error!("{}", e.display_chain());
    }
}
