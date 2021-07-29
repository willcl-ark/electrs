use bitcoin::blockdata::transaction::Transaction;
use bitcoin::consensus::encode::{deserialize, serialize};
use bitcoin::hashes::hex::{FromHex, ToHex};
use bitcoin::hashes::{sha256d::Hash as Sha256dHash, Hash};
use error_chain::ChainedError;
use serde_cbor::{de, value::from_value, to_vec, Value, Value::Array};
use serde_derive::{Deserialize, Serialize};
use std::collections::{HashMap, BTreeMap};
use std::io::{BufRead, BufReader, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread;

use crate::errors::*;
use crate::metrics::{Gauge, HistogramOpts, HistogramVec, MetricOpts, Metrics};
use crate::query::{Query, Status};
use crate::util::{spawn_thread, Channel, HeaderEntry};

const ELECTRS_VERSION: &str = env!("CARGO_PKG_VERSION");
const PROTOCOL_VERSION: &str = "1.4";

struct Connection {
    query: Arc<Query>,
    last_header_entry: Option<HeaderEntry>,
    status_hashes: HashMap<Sha256dHash, Value>, // ScriptHash -> StatusHash
    stream: TcpStream,
    addr: SocketAddr,
    sender: SyncSender<Message>,
    stats: Arc<Stats>,
    relayfee: f64,
}

impl Connection {
    pub fn new(
        query: Arc<Query>,
        stream: TcpStream,
        addr: SocketAddr,
        stats: Arc<Stats>,
        relayfee: f64,
        sender: SyncSender<Message>,
    ) -> Connection {
        Connection {
            query,
            last_header_entry: None, // disable header subscription for now
            status_hashes: HashMap::new(),
            stream,
            addr,
            sender,
            stats,
            relayfee,
        }
    }

    // fn blockchain_headers_subscribe(&mut self) -> Result<Value> {
    //     let entry = self.query.get_best_header()?;
    //     let hex_header = hex::encode(serialize(entry.header()));
    //     let result = json!({"hex": hex_header, "height": entry.height()});
    //     self.last_header_entry = Some(entry);
    //     Ok(result)
    // }

    fn server_version(&self, params: &[serde_cbor::value::Value]) -> Result<Value> {
        if params.len() != 2 {
            bail!("invalid params: {:?}", params);
        }
        Ok(serde_cbor::value::to_value(format!("{}, {}", ELECTRS_VERSION, PROTOCOL_VERSION)).unwrap())
    }

    // fn server_banner(&self) -> Result<Value> {
    //     Ok(json!(self.query.get_banner()?))
    // }
    //
    // fn server_donation_address(&self) -> Result<Value> {
    //     Ok(Value::Null)
    // }
    //
    // fn server_peers_subscribe(&self) -> Result<Value> {
    //     Ok(json!([]))
    // }
    //
    // fn mempool_get_fee_histogram(&self) -> Result<Value> {
    //     Ok(json!(self.query.get_fee_histogram()))
    // }
    //
    // fn blockchain_block_header(&self, params: &[Value]) -> Result<Value> {
    //     let height = usize_from_value(params.get(0), "height")?;
    //     let cp_height = usize_from_value_or(params.get(1), "cp_height", 0)?;
    //
    //     let raw_header_hex: String = self
    //         .query
    //         .get_headers(&[height])
    //         .into_iter()
    //         .map(|entry| hex::encode(&serialize(entry.header())))
    //         .collect();
    //
    //     if cp_height == 0 {
    //         return Ok(json!(raw_header_hex));
    //     }
    //     let (branch, root) = self.query.get_header_merkle_proof(height, cp_height)?;
    //
    //     let branch_vec: Vec<String> = branch.into_iter().map(|b| b.to_hex()).collect();
    //
    //     Ok(json!({
    //         "header": raw_header_hex,
    //         "root": root.to_hex(),
    //         "branch": branch_vec
    //     }))
    // }
    //
    // fn blockchain_block_headers(&self, params: &[Value]) -> Result<Value> {
    //     let start_height = usize_from_value(params.get(0), "start_height")?;
    //     let count = usize_from_value(params.get(1), "count")?;
    //     let cp_height = usize_from_value_or(params.get(2), "cp_height", 0)?;
    //     let heights: Vec<usize> = (start_height..(start_height + count)).collect();
    //     let headers: Vec<String> = self
    //         .query
    //         .get_headers(&heights)
    //         .into_iter()
    //         .map(|entry| hex::encode(&serialize(entry.header())))
    //         .collect();
    //
    //     if count == 0 || cp_height == 0 {
    //         return Ok(json!({
    //             "count": headers.len(),
    //             "hex": headers.join(""),
    //             "max": 2016,
    //         }));
    //     }
    //
    //     let (branch, root) = self
    //         .query
    //         .get_header_merkle_proof(start_height + (count - 1), cp_height)?;
    //
    //     let branch_vec: Vec<String> = branch.into_iter().map(|b| b.to_hex()).collect();
    //
    //     Ok(json!({
    //         "count": headers.len(),
    //         "hex": headers.join(""),
    //         "max": 2016,
    //         "root": root.to_hex(),
    //         "branch" : branch_vec
    //     }))
    // }

    // fn blockchain_estimatefee(&self, params: &[Value]) -> Result<Value> {
    //     let blocks_count = usize_from_value(params.get(0), "blocks_count")?;
    //     let fee_rate = self.query.estimate_fee(blocks_count); // in BTC/kB
    //     Ok(json!(fee_rate.max(self.relayfee)))
    // }
    //
    // fn blockchain_relayfee(&self) -> Result<Value> {
    //     Ok(json!(self.relayfee)) // in BTC/kB
    // }
    //
    // fn blockchain_scripthash_subscribe(&mut self, params: &[Value]) -> Result<Value> {
    //     let script_hash =
    //         hash_from_value::<Sha256dHash>(params.get(0)).chain_err(|| "bad script_hash")?;
    //     let status = self.query.status(&script_hash[..])?;
    //     let result = status.hash().map_or(Value::Null, |h| json!(hex::encode(h)));
    //     if self
    //         .status_hashes
    //         .insert(script_hash, result.clone())
    //         .is_none()
    //     {
    //         self.stats.subscriptions.inc();
    //     }
    //
    //     Ok(result)
    // }
    //
    // fn blockchain_scripthash_get_balance(&self, params: &[Value]) -> Result<Value> {
    //     let script_hash =
    //         hash_from_value::<Sha256dHash>(params.get(0)).chain_err(|| "bad script_hash")?;
    //     let status = self.query.status(&script_hash[..])?;
    //     Ok(
    //         json!({ "confirmed": status.confirmed_balance(), "unconfirmed": status.mempool_balance() }),
    //     )
    // }
    //
    // fn blockchain_scripthash_get_history(&self, params: &[Value]) -> Result<Value> {
    //     let script_hash =
    //         hash_from_value::<Sha256dHash>(params.get(0)).chain_err(|| "bad script_hash")?;
    //     let status = self.query.status(&script_hash[..])?;
    //     Ok(json!(Value::Array(
    //         status
    //             .history()
    //             .into_iter()
    //             .map(|item| item.to_json())
    //             .collect()
    //     )))
    // }
    //
    // fn blockchain_scripthash_listunspent(&self, params: &[Value]) -> Result<Value> {
    //     let script_hash =
    //         hash_from_value::<Sha256dHash>(params.get(0)).chain_err(|| "bad script_hash")?;
    //     Ok(unspent_from_status(&self.query.status(&script_hash[..])?))
    // }
    //
    // fn blockchain_transaction_broadcast(&self, params: &[Value]) -> Result<Value> {
    //     let tx = params.get(0).chain_err(|| "missing tx")?;
    //     let tx = tx.as_str().chain_err(|| "non-string tx")?;
    //     let tx = hex::decode(&tx).chain_err(|| "non-hex tx")?;
    //     let tx: Transaction = deserialize(&tx).chain_err(|| "failed to parse tx")?;
    //     let txid = self.query.broadcast(&tx)?;
    //     self.query.update_mempool()?;
    //     if let Err(e) = self.sender.try_send(Message::PeriodicUpdate) {
    //         warn!("failed to issue PeriodicUpdate after broadcast: {}", e);
    //     }
    //     Ok(json!(txid.to_hex()))
    // }
    //
    // fn blockchain_transaction_get(&self, params: &[Value]) -> Result<Value> {
    //     let tx_hash = hash_from_value(params.get(0)).chain_err(|| "bad tx_hash")?;
    //     let verbose = match params.get(1) {
    //         Some(value) => value.as_bool().chain_err(|| "non-bool verbose value")?,
    //         None => false,
    //     };
    //     Ok(self.query.get_transaction(&tx_hash, verbose)?)
    // }
    //
    // fn blockchain_transaction_get_confirmed_blockhash(&self, params: &[Value]) -> Result<Value> {
    //     let tx_hash = hash_from_value(params.get(0)).chain_err(|| "bad tx_hash")?;
    //     self.query.get_confirmed_blockhash(&tx_hash)
    // }
    //
    // fn blockchain_transaction_get_merkle(&self, params: &[Value]) -> Result<Value> {
    //     let tx_hash = hash_from_value(params.get(0)).chain_err(|| "bad tx_hash")?;
    //     let height = usize_from_value(params.get(1), "height")?;
    //     let (merkle, pos) = self
    //         .query
    //         .get_merkle_proof(&tx_hash, height)
    //         .chain_err(|| "cannot create merkle proof")?;
    //     let merkle: Vec<String> = merkle.into_iter().map(|txid| txid.to_hex()).collect();
    //     Ok(json!({
    //             "block_height": height,
    //             "merkle": merkle,
    //             "pos": pos}))
    // }
    //
    // fn blockchain_transaction_id_from_pos(&self, params: &[Value]) -> Result<Value> {
    //     let height = usize_from_value(params.get(0), "height")?;
    //     let tx_pos = usize_from_value(params.get(1), "tx_pos")?;
    //     let want_merkle = bool_from_value_or(params.get(2), "merkle", false)?;
    //
    //     let (txid, merkle) = self.query.get_id_from_pos(height, tx_pos, want_merkle)?;
    //
    //     if !want_merkle {
    //         return Ok(json!(txid.to_hex()));
    //     }
    //
    //     let merkle_vec: Vec<String> = merkle.into_iter().map(|entry| entry.to_hex()).collect();
    //
    //     Ok(json!({
    //         "tx_hash" : txid.to_hex(),
    //         "merkle" : merkle_vec}))
    // }

    fn handle_command(&mut self, method: &str, params: &[serde_cbor::Value], id: &serde_cbor::Value) -> serde_cbor::Result<Value> {
        let timer = self
            .stats
            .latency
            .with_label_values(&[method])
            .start_timer();
        let result: Result<serde_cbor::Value> = match method {
            // "blockchain.block.header" => self.blockchain_block_header(&params),
            // "blockchain.block.headers" => self.blockchain_block_headers(&params),
            // "blockchain.estimatefee" => self.blockchain_estimatefee(&params),
            // "blockchain.headers.subscribe" => self.blockchain_headers_subscribe(),
            // "blockchain.relayfee" => self.blockchain_relayfee(),
            // "blockchain.scripthash.get_balance" => self.blockchain_scripthash_get_balance(&params),
            // "blockchain.scripthash.get_history" => self.blockchain_scripthash_get_history(&params),
            // "blockchain.scripthash.listunspent" => self.blockchain_scripthash_listunspent(&params),
            // "blockchain.scripthash.subscribe" => self.blockchain_scripthash_subscribe(&params),
            // "blockchain.transaction.broadcast" => self.blockchain_transaction_broadcast(&params),
            // "blockchain.transaction.get" => self.blockchain_transaction_get(&params),
            // "blockchain.transaction.get_merkle" => self.blockchain_transaction_get_merkle(&params),
            // "blockchain.transaction.get_confirmed_blockhash" => {
            //     self.blockchain_transaction_get_confirmed_blockhash(&params)
            // }
            // "blockchain.transaction.id_from_pos" => {
            //     self.blockchain_transaction_id_from_pos(&params)
            // }
            // "mempool.get_fee_histogram" => self.mempool_get_fee_histogram(),
            // "server.banner" => self.server_banner(),
            // "server.donation_address" => self.server_donation_address(),
            // "server.peers.subscribe" => self.server_peers_subscribe(),
            "server.ping" => Ok(Value::Null),
            "server.version" => self.server_version(&params),
            &_ => Err(Error::from("bad command")),
        };
        timer.observe_duration();
        // TODO: return application errors should be sent to the client
        Ok(match result {
            Ok(result) => {
                let map: BTreeMap<Value, Value> = btreemap! {
                    Value::from("cborrpc".to_string()) => Value::from("0.1".to_string()),
                    Value::from("id".to_string()) => id.clone(),
                    Value::from("result".to_string()) => result.clone() ,
                };
                Value::from(map)
            }
            Err(e) => {
                warn!(
                    "rpc #{:?} {} {:?} failed: {}",
                    id,
                    method,
                    params,
                    e.display_chain()
                );
                let map: BTreeMap<Value, Value> = btreemap! {
                    Value::from("cborrpc".to_string()) => Value::from("0.01".to_string()),
                    Value::from("id".to_string()) => id.clone(),
                    Value::from("error".to_string()) => Value::from(format!("{}", e).to_string()) ,
                };
                Value::from(map)
            }
        })
    }

    fn send_values(&mut self, values: &[Value]) -> Result<()> {
        for value in values {
            self.stream
                // Serialise the CBOR Value to a vector of u8 and send it
                .write_all(&*to_vec(value).unwrap())
                .chain_err(|| format!("failed to send {:?}", value))?;
        }
        Ok(())
    }

    fn handle_replies(&mut self, receiver: Receiver<Message>) -> Result<()> {
        let empty_params = Value::Array([].to_vec());
        loop {
            let msg = receiver.recv().chain_err(|| "channel closed")?;
            trace!("RPC {:?}", msg);
            match msg {
                Message::Request(cmd) => {
                    let reply = match (
                        cmd.get(&Value::Text(String::from("method"))),
                        cmd.get(&Value::Text(String::from("params"))).unwrap_or_else(|| &empty_params),
                        cmd.get(&Value::Text(String::from("id"))),
                    ) {
                        (
                            Some(&Value::Text(ref method)),
                            &Value::Array(ref params),
                            Some(ref id),
                        ) => self.handle_command(&method, &params, &id).unwrap(),
                        _ => bail!("invalid command: {:?}", cmd),
                    };
                    self.send_values(&[reply])?
                }
                Message::PeriodicUpdate => return Ok(()),
                Message::Done => return Ok(()),
            }
        }
    }

    fn parse_requests(mut reader: BufReader<TcpStream>, tx: SyncSender<Message>) -> Result<()> {
        loop {
            //Read a cbor value from a reader
            let mut line = Vec::<u8>::new();
            reader
                .read_until(b'\n', &mut line)
                .chain_err(|| "failed to read a request")?;
            if line.is_empty() {
                tx.send(Message::Done).chain_err(|| "channel closed")?;
                return Ok(());
            } else {
                if line.starts_with(&[22, 3, 1]) {
                    // (very) naive SSL handshake detection
                    let _ = tx.send(Message::Done);
                    bail!("invalid request - maybe SSL-encrypted data?: {:?}", line)
                }
                //match String::from_utf8(line) {
                match serde_cbor::from_slice(&line).chain_err(|| "invalid CBOR format") {
                    Ok(req) => tx
                        .send(Message::Request(req))
                        .chain_err(|| "channel closed")?,
                    Err(err) => {
                        let _ = tx.send(Message::Done);
                        bail!("invalid UTF8: {}", err)
                    }
                }
            }
            // let mut line = Vec::<u8>::new();
            // reader
            //     .read_until(b'\n', &mut line)
            //     .chain_err(|| "failed to read a request")?;
            // if line.is_empty() {
            //     tx.send(Message::Done).chain_err(|| "channel closed")?;
            //     return Ok(());
            // } else {
            //     if line.starts_with(&[22, 3, 1]) {
            //         // (very) naive SSL handshake detection
            //         let _ = tx.send(Message::Done);
            //         bail!("invalid request - maybe SSL-encrypted data?: {:?}", line)
            //     }
            //     match String::from_utf8(line) {
            //         Ok(req) => tx
            //             .send(Message::Request(req))
            //             .chain_err(|| "channel closed")?,
            //         Err(err) => {
            //             let _ = tx.send(Message::Done);
            //             bail!("invalid UTF8: {}", err)
            //         }
            //     }
            // }
        }
    }

    pub fn run(mut self, receiver: Receiver<Message>) {
        let reader = BufReader::new(self.stream.try_clone().expect("failed to clone TcpStream"));
        let sender = self.sender.clone();
        let child = spawn_thread("reader", || Connection::parse_requests(reader, sender));
        if let Err(e) = self.handle_replies(receiver) {
            error!(
                "[{}] connection handling failed: {}",
                self.addr,
                e.display_chain().to_string()
            );
        }
        self.stats
            .subscriptions
            .sub(self.status_hashes.len() as i64);
        debug!("[{}] shutting down connection", self.addr);
        let _ = self.stream.shutdown(Shutdown::Both);
        if let Err(err) = child.join().expect("receiver panicked") {
            error!("[{}] receiver failed: {}", self.addr, err);
        }
    }
}

#[derive(Debug)]
pub enum Message {
    Request(BTreeMap<Value, Value>),
    PeriodicUpdate,
    Done,
}

pub enum Notification {
    Periodic,
    Exit,
}

pub struct RPC {
    notification: Sender<Notification>,
    server: Option<thread::JoinHandle<()>>, // so we can join the server while dropping this object
}

struct Stats {
    latency: HistogramVec,
    subscriptions: Gauge,
}

impl RPC {
    fn start_notifier(
        notification: Channel<Notification>,
        senders: Arc<Mutex<Vec<SyncSender<Message>>>>,
        acceptor: Sender<Option<(TcpStream, SocketAddr)>>,
    ) {
        spawn_thread("notification", move || {
            for msg in notification.receiver().iter() {
                let mut senders = senders.lock().unwrap();
                match msg {
                    Notification::Periodic => {
                        senders.retain(|sender| {
                            if let Err(TrySendError::Disconnected(_)) =
                                sender.try_send(Message::PeriodicUpdate)
                            {
                                false // drop disconnected clients
                            } else {
                                true
                            }
                        })
                    }
                    Notification::Exit => acceptor.send(None).unwrap(), // mark acceptor as done
                }
            }
        });
    }

    fn start_acceptor(addr: SocketAddr) -> Channel<Option<(TcpStream, SocketAddr)>> {
        let chan = Channel::unbounded();
        let acceptor = chan.sender();
        spawn_thread("acceptor", move || {
            let listener =
                TcpListener::bind(addr).unwrap_or_else(|e| panic!("bind({}) failed: {}", addr, e));
            info!(
                "Electrum RPC server running on {} (protocol {})",
                addr, PROTOCOL_VERSION
            );
            loop {
                let (stream, addr) = listener.accept().expect("accept failed");
                stream
                    .set_nonblocking(false)
                    .expect("failed to set connection as blocking");
                acceptor.send(Some((stream, addr))).expect("send failed");
            }
        });
        chan
    }

    pub fn start(addr: SocketAddr, query: Arc<Query>, metrics: &Metrics, relayfee: f64) -> RPC {
        let stats = Arc::new(Stats {
            latency: metrics.histogram_vec(
                HistogramOpts::new("electrs_electrum_rpc", "Electrum RPC latency (seconds)"),
                &["method"],
            ),
            subscriptions: metrics.gauge(MetricOpts::new(
                "electrs_electrum_subscriptions",
                "# of Electrum subscriptions",
            )),
        });
        stats.subscriptions.set(0);
        let notification = Channel::unbounded();

        RPC {
            notification: notification.sender(),
            server: Some(spawn_thread("rpc", move || {
                let senders = Arc::new(Mutex::new(Vec::<SyncSender<Message>>::new()));

                let acceptor = RPC::start_acceptor(addr);
                RPC::start_notifier(notification, senders.clone(), acceptor.sender());

                let mut threads = HashMap::new();
                let (garbage_sender, garbage_receiver) = crossbeam_channel::unbounded();

                while let Some((stream, addr)) = acceptor.receiver().recv().unwrap() {
                    // explicitely scope the shadowed variables for the new thread
                    let query = Arc::clone(&query);
                    let stats = Arc::clone(&stats);
                    let garbage_sender = garbage_sender.clone();
                    let (sender, receiver) = mpsc::sync_channel(10);

                    senders.lock().unwrap().push(sender.clone());

                    let spawned = spawn_thread("peer", move || {
                        info!("[{}] connected peer", addr);
                        let conn = Connection::new(query, stream, addr, stats, relayfee, sender);
                        conn.run(receiver);
                        info!("[{}] disconnected peer", addr);
                        let _ = garbage_sender.send(std::thread::current().id());
                    });

                    trace!("[{}] spawned {:?}", addr, spawned.thread().id());
                    threads.insert(spawned.thread().id(), spawned);
                    while let Ok(id) = garbage_receiver.try_recv() {
                        if let Some(thread) = threads.remove(&id) {
                            trace!("[{}] joining {:?}", addr, id);
                            if let Err(error) = thread.join() {
                                error!("failed to join {:?}: {:?}", id, error);
                            }
                        }
                    }
                }
                trace!("closing {} RPC connections", senders.lock().unwrap().len());
                for sender in senders.lock().unwrap().iter() {
                    let _ = sender.send(Message::Done);
                }
                for (id, thread) in threads {
                    trace!("joining {:?}", id);
                    if let Err(error) = thread.join() {
                        error!("failed to join {:?}: {:?}", id, error);
                    }
                }

                trace!("RPC connections are closed");
            })),
        }
    }

    pub fn notify(&self) {
        self.notification.send(Notification::Periodic).unwrap();
    }
}

impl Drop for RPC {
    fn drop(&mut self) {
        trace!("stop accepting new RPCs");
        self.notification.send(Notification::Exit).unwrap();
        if let Some(handle) = self.server.take() {
            handle.join().unwrap();
        }
        trace!("RPC server is stopped");
    }
}
