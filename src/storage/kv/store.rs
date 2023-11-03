use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;

use crate::storage::index::art::KV;
use crate::storage::index::KeyTrait;
use crate::storage::kv::entry::{TxRecord, ValueRef};
use crate::storage::kv::error::{Error, Result};
use crate::storage::kv::indexer::Indexer;
use crate::storage::kv::option::Options;
use crate::storage::kv::oracle::Oracle;
use crate::storage::kv::reader::{Reader, TxReader};
use crate::storage::kv::transaction::{Mode, Transaction};
use crate::storage::log::aol::aol::AOL;
use crate::storage::log::wal::wal::WAL;
use crate::storage::log::Error as LogError;
use crate::storage::log::{Options as LogOptions, BLOCK_SIZE};

/// An MVCC-based transactional key-value store.
pub struct Store<P: KeyTrait> {
    pub(crate) core: Arc<Core<P>>,
}

impl<P: KeyTrait> Store<P> {
    /// Creates a new MVCC key-value store with the given key-value store for storage.
    pub fn new(opts: Options) -> Result<Self> {
        let core = Arc::new(Core::new(opts)?);
        Ok(Self { core })
    }

    pub fn begin(self: &Arc<Self>) -> Result<Transaction<P>> {
        let mut txn = Transaction::new(self.core.clone(), Mode::ReadWrite)?;
        txn.read_ts = self.core.oracle.read_ts();
        Ok(txn)
    }

    pub fn begin_with_mode(self: &Arc<Self>, mode: Mode) -> Result<Transaction<P>> {
        let mut txn = Transaction::new(self.core.clone(), mode)?;
        txn.read_ts = self.core.oracle.read_ts();
        Ok(txn)
    }

    pub fn view(self: Arc<Self>, f: impl FnOnce(&mut Transaction<P>) -> Result<()>) -> Result<()> {
        let mut txn = self.begin_with_mode(Mode::ReadOnly)?;
        f(&mut txn)?;

        Ok(())
    }

    pub fn write(self: Arc<Self>, f: impl FnOnce(&mut Transaction<P>) -> Result<()>) -> Result<()> {
        let mut txn = self.begin_with_mode(Mode::ReadWrite)?;
        f(&mut txn)?;
        txn.commit()?;

        Ok(())
    }

    pub fn closed(&self) -> bool {
        self.core.closed
    }
}

pub struct Core<P: KeyTrait> {
    /// Index for store.
    pub(crate) indexer: RwLock<Indexer<P>>,
    /// Options for store.
    pub(crate) opts: Options,
    /// WAL for store.
    pub(crate) wal: Option<Arc<WAL>>,
    /// Transaction log for store.
    pub(crate) tlog: Arc<RwLock<AOL>>,
    /// Transaction ID Oracle for store.
    pub(crate) oracle: Arc<Oracle<P>>,
    /// Flag to indicate if store is closed.
    pub(crate) closed: bool,
}

impl<P> Core<P>
where
    P: KeyTrait,
{
    pub fn new(opts: Options) -> Result<Self> {
        let topts = LogOptions::default();

        let tlog = AOL::open(&opts.dir, &topts)?;
        let mut indexer = Indexer::new();

        if tlog.size()? > 0 {
            Core::load_index(&opts, &mut indexer)?;
        }

        let oracle = Oracle::new(&opts);
        oracle.set_ts(indexer.version());

        Ok(Self {
            indexer: RwLock::new(indexer),
            opts,
            wal: None,
            tlog: Arc::new(RwLock::new(tlog)),
            oracle: Arc::new(oracle),
            closed: false,
        })
    }

    fn load_index(opts: &Options, indexer: &mut Indexer<P>) -> Result<()> {
        let tlog = AOL::open(&opts.dir, &LogOptions::default())?;
        let reader = Reader::new_from(tlog, 0, BLOCK_SIZE)?;
        let mut tx_reader = TxReader::new(reader)?;
        let mut tx = TxRecord::new(opts.max_tx_entries);

        loop {
            // Reset the transaction record before reading into it.
            // Keeping the same transaction record instance avoids
            // unnecessary allocations.
            tx.reset();

            // Read the next transaction record from the log.
            let res = tx_reader.read_into(&mut tx);
            if let Err(e) = res {
                if let Error::Log(log_error) = &e {
                    match log_error {
                        LogError::EOF => break,
                        _ => return Err(e),
                    }
                } else {
                    return Err(e);
                }
            }

            let (_tx_offset, value_offsets) = res.unwrap();
            Core::process_entries(&tx, opts, &value_offsets, indexer)?;
        }

        Ok(())
    }

    fn process_entries(
        tx: &TxRecord,
        opts: &Options,
        value_offsets: &HashMap<Bytes, usize>,
        indexer: &mut Indexer<P>,
    ) -> Result<()> {
        let mut kv_pairs: Vec<KV<P, Bytes>> = Vec::new();

        for entry in &tx.entries {
            let index_value = ValueRef::<P>::encode(
                &entry.key,
                &entry.value,
                entry.metadata.as_ref(),
                value_offsets,
                opts.max_value_threshold,
            );

            kv_pairs.push(KV {
                key: entry.key[..].into(),
                value: index_value,
                version: tx.header.id,
                ts: tx.header.ts,
            });
        }

        indexer.bulk_insert(&kv_pairs)
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::index::VectorKey;
    use crate::storage::kv::option::Options;
    use crate::storage::kv::store::Store;

    #[test]
    fn test_new_store() {
        let opts = Options::new();
        let store = Store::<VectorKey>::new(opts).expect("should create store");
        assert!(!store.closed());
    }
}
