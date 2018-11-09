/// Benchmark full scan and point lookups.
extern crate electrs;

#[macro_use]
extern crate log;

extern crate bitcoin;
extern crate error_chain;

use bitcoin::util::hash::Sha256dHash;
use electrs::{
    config::Config,
    daemon::Daemon,
    errors::*,
    index::TxRow,
    metrics::Metrics,
    signal::Waiter,
    store::{DBStore, ReadStore},
};

use error_chain::ChainedError;
use std::time::{Duration, Instant};

fn run(config: Config) -> Result<()> {
    if !config.db_path.exists() {
        panic!(
            "DB {:?} must exist when running this benchmark!",
            config.db_path
        );
    }
    let store = DBStore::open(&config.db_path, /*low_memory=*/ false);
    let signal = Waiter::new();
    let metrics = Metrics::new(config.monitoring_addr);
    metrics.start();

    let daemon = Daemon::new(
        &config.daemon_dir,
        config.daemon_rpc_addr,
        config.cookie_getter(),
        config.network_type,
        signal.clone(),
        &metrics,
    )?;

    let mut blockhash =
        Sha256dHash::from_hex("00000000000000000133e9920d5d808d430e325027d513420e7fc2f7da231ffe")
            .unwrap();

    for _ in 0..100 {
        let block = daemon.getblock(&blockhash)?;
        let txhashes: Vec<Sha256dHash> = block.txdata.iter().map(|tx| tx.txid()).collect();
        let len = txhashes.len() as u32;

        let now = Instant::now();
        for txhash in &txhashes {
            let key = TxRow::filter_full(txhash);
            let value = store.get(&key);
            value.chain_err(|| format!("missing key {}", txhash))?;
        }
        let elapsed = now.elapsed();
        info!("got {} txs @ {:?} secs ({:?} per tx)", len, elapsed, elapsed / len);
        blockhash = block.header.prev_blockhash;
    }
    Ok(())
}

fn main() {
    if let Err(e) = run(Config::from_args()) {
        error!("{}", e.display_chain());
    }
}
