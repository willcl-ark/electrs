extern crate electrs;

extern crate bitcoin;
extern crate hex;

#[macro_use]
extern crate log;

use bitcoin::consensus::encode::deserialize;
use bitcoin::util::hash::Sha256dHash;

use electrs::{
    config::Config, index::TxOutRow, query::txrows_by_prefix, store::DBStore, util::HashPrefix,
};

fn revhex(value: &[u8]) -> String {
    hex::encode(&value.iter().cloned().rev().collect::<Vec<u8>>())
}

fn popular_scripthash(store: DBStore) {
    let prefix = b"O"; // funding outputs

    let mut last_script_hash_prefix = HashPrefix::default();
    let mut last_txid_prefix = HashPrefix::default();
    let mut txids_count = 0;
    let mut max_txids_count = 0;

    for row in store.iter_scan(prefix) {
        assert!(row.key.starts_with(prefix));
        let out_row = TxOutRow::from_row(&row);
        if last_script_hash_prefix == out_row.key.script_hash_prefix {
            txids_count += 1;
        } else {
            if txids_count > max_txids_count {
                let txrows = txrows_by_prefix(&store, &last_txid_prefix);
                max_txids_count = txids_count;
                info!(
                    "{}: {} => {:?}",
                    hex::encode(&last_script_hash_prefix),
                    max_txids_count,
                    txrows.first().map(|txrow| revhex(&txrow.key.txid)),
                )
            }
            txids_count = 0;
        }
        last_script_hash_prefix = out_row.key.script_hash_prefix;
        last_txid_prefix = out_row.txid_prefix;
    }
}

fn run(config: Config) {
    if !config.db_path.exists() {
        panic!("DB {:?} must exist when running this tool!", config.db_path);
    }
    let store = DBStore::open(&config.db_path, /*low_memory=*/ false);
    popular_scripthash(store);
}

fn main() {
    run(Config::from_args());
}
