/// Test blk*.dat parsing.
extern crate electrs;

extern crate error_chain;
extern crate stderrlog;

use electrs::{bulk, errors::*};

fn run(files: Vec<String>, magic: u32) -> Result<()> {
    for f in files {
        dbg!(&f);
        let blob = std::fs::read(&f).expect("failed to read file");
        let blocks = bulk::parse_blocks(blob, magic)?;
        dbg!(blocks.len());
    }
    Ok(())
}

fn main() {
    let mut log = stderrlog::new();
    log.verbosity(4);
    log.timestamp(stderrlog::Timestamp::Millisecond);
    log.init().expect("logging initialization failed");

    run(std::env::args().skip(1).collect(), 0xD9B4BEF9).expect("failed to parse");
}
