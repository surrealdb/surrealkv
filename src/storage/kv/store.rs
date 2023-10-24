use std::sync::{Arc, RwLock};
use std::collections::HashMap;

use bytes::Bytes;

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
pub struct MVCCStore<P: KeyTrait, V: Clone + AsRef<Bytes> + From<bytes::Bytes>> {
    pub(crate) core: Arc<Core<P, V>>,
}

impl<P: KeyTrait, V: Clone + AsRef<Bytes> + From<bytes::Bytes>> MVCCStore<P, V> {
    /// Creates a new MVCC key-value store with the given key-value store for storage.
    pub fn new(opts: Options) -> Result<Self> {
        let core = Arc::new(Core::new(opts)?);
        Ok(Self { core })
    }

    pub fn begin<'a>(self: &'a Arc<Self>) -> Result<Transaction<'a, P, V>> {
        let mut txn = Transaction::new(self.core.clone(), Mode::ReadWrite)?;
        txn.read_ts = self.core.oracle.read_ts();
        Ok(txn)
    }

    pub fn begin_with_mode<'a>(self: &'a Arc<Self>, mode: Mode) -> Result<Transaction<'a, P, V>> {
        let mut txn = Transaction::new(self.core.clone(), mode)?;
        txn.read_ts = self.core.oracle.read_ts();
        Ok(txn)
    }

    pub fn view(
        self: Arc<Self>,
        f: impl FnOnce(&mut Transaction<P, V>) -> Result<()>,
    ) -> Result<()> {
        let mut txn = self.begin_with_mode(Mode::ReadOnly)?;
        f(&mut txn)?;

        Ok(())
    }

    pub fn write(
        self: Arc<Self>,
        f: impl FnOnce(&mut Transaction<P, V>) -> Result<()>,
    ) -> Result<()> {
        let mut txn = self.begin_with_mode(Mode::ReadWrite)?;
        f(&mut txn)?;
        txn.commit()?;

        Ok(())
    }

    pub fn closed(&self) -> bool {
        self.core.closed
    }
}

pub struct Core<P: KeyTrait, V: Clone + AsRef<Bytes> + From<bytes::Bytes>> {
    /// Index for store.
    pub(crate) indexer: RwLock<Indexer<P, V>>,
    /// Options for store.
    pub(crate) opts: Options,
    /// WAL for store.
    pub(crate) wal: Option<Arc<WAL>>,
    /// Transaction log for store.
    pub(crate) tlog: Arc<RwLock<AOL>>,
    /// Transaction ID Oracle for store.
    pub(crate) oracle: Arc<Oracle>,
    /// Flag to indicate if store is closed.
    pub(crate) closed: bool,
}

impl<P, V> Core<P, V>
where
    P: KeyTrait,
    V: Clone + AsRef<Bytes> + From<bytes::Bytes>,
{
    pub fn new(opts: Options) -> Result<Self> {
        let topts = LogOptions::default();

        let tlog = AOL::open(&opts.dir, &topts)?;
        let mut indexer = Indexer::new();

        if tlog.size()? > 0 {
            Core::load_index(&opts,  &mut indexer)?;
        }

        let oracle = Oracle::new();
        oracle.set_txn_id(indexer.version());

        Ok(Self {
            indexer: RwLock::new(indexer),
            opts,
            wal: None,
            tlog: Arc::new(RwLock::new(tlog)),
            oracle: Arc::new(oracle),
            closed: false,
        })
    }

    fn load_index(
        opts: &Options,
        indexer: &mut Indexer<P, V>,
    ) -> Result<()> {
        let tlog = AOL::open(&opts.dir, &LogOptions::default())?;
        let reader = Reader::new_from(tlog, 0, BLOCK_SIZE)?;
        let mut tx_reader = TxReader::new(reader)?;
        let mut tx = TxRecord::new(opts.max_tx_entries);

        loop {
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

            let (tx_offset, value_offsets) = res.unwrap();
            Core::process_entries(&tx, &opts, &value_offsets, indexer)?;
        }

        Ok(())
    }

    fn process_entries(
        tx: &TxRecord,
        opts: &Options,
        value_offsets: &HashMap<Bytes, usize>,
        indexer: &mut Indexer<P,V>,
    ) -> Result<()>{
        let mut kv_pairs: Vec<KV<P, V>> = Vec::new();

        for entry in &tx.entries {
            let index_value = ValueRef::<P, V>::encode(
                &entry.key,
                &entry.value,
                entry.metadata.as_ref(),
                value_offsets,
                opts.max_value_threshold,
            );

            kv_pairs.push(KV {
                key: entry.key[..].into(),
                value: index_value.into(),
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
    use crate::storage::kv::store::MVCCStore;
    use crate::storage::kv::util::NoopValue;

    #[test]
    fn test_new_store() {
        let opts = Options::new();
        let store = MVCCStore::<VectorKey, NoopValue>::new(opts).expect("should create store");
        assert_eq!(store.closed(), false);
    }
}
