use redb::ReadableTable;
use redb::TableDefinition;
use rocksdb::{Direction, IteratorMode, TransactionDB, TransactionOptions, WriteOptions};
use std::fs;
use std::fs::File;
use std::path::Path;

const X: TableDefinition<&[u8], &[u8]> = TableDefinition::new("x");

pub(crate) trait BenchStore {
    type T<'db>: Transaction
    where
        Self: 'db;

    fn transaction(&self, write: bool) -> Self::T<'_>;
}

pub(crate) trait Transaction {
    type Output<'out>: AsRef<[u8]> + 'out
    where
        Self: 'out;

    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()>;
    fn get<'a>(&'a self, key: &[u8]) -> Option<Self::Output<'a>>;
    fn range(&self, key: &[u8]) -> Vec<Vec<u8>>;
    fn delete(&mut self, key: &[u8]) -> Result<(), ()>;
    async fn commit(self) -> Result<(), ()>;
}

pub struct RedbBenchStore<'a> {
    db: &'a redb::Database,
}

impl<'a> RedbBenchStore<'a> {
    pub fn new(db: &'a redb::Database) -> Self {
        RedbBenchStore { db }
    }
}

impl<'a> BenchStore for RedbBenchStore<'a> {
    type T<'db> = RedbTransaction<'db> where Self: 'db;

    fn transaction(&self, write: bool) -> Self::T<'_> {
        if write {
            let txn = self.db.begin_write().unwrap();
            RedbTransaction::Write(txn)
        } else {
            let txn = self.db.begin_read().unwrap();
            RedbTransaction::Read(txn)
        }
    }
}

pub enum RedbTransaction<'a> {
    Read(redb::ReadTransaction<'a>),
    Write(redb::WriteTransaction<'a>),
}

impl<'a> Transaction for RedbTransaction<'a> {
    type Output<'out> = Vec<u8> where Self: 'out;

    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        match self {
            RedbTransaction::Read(_) => Err(()),
            RedbTransaction::Write(txn) => {
                let mut table = txn.open_table(X).unwrap();
                table.insert(key, value).map(|_| ()).map_err(|_| ())
            }
        }
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), ()> {
        match self {
            RedbTransaction::Read(_) => Err(()),
            RedbTransaction::Write(txn) => {
                let mut table = txn.open_table(X).unwrap();
                table.remove(key).map(|_| ()).map_err(|_| ())
            }
        }
    }

    fn get<'b>(&'b self, key: &[u8]) -> Option<Self::Output<'b>> {
        match self {
            RedbTransaction::Write(_) => None,
            RedbTransaction::Read(txn) => {
                let table = txn.open_table(X).unwrap();
                let res = table.get(key).unwrap();
                res.map(|value| value.value().to_vec())
            }
        }
    }

    fn range(&self, key: &[u8]) -> Vec<Vec<u8>> {
        match self {
            RedbTransaction::Write(_) => Vec::new(),
            RedbTransaction::Read(txn) => {
                let table = txn.open_table(X).unwrap();
                let iter: redb::Range<'_, &[u8], &[u8]> = table.range(key..).unwrap();
                let values: Result<Vec<Vec<u8>>, _> = iter
                    .map(|result| match result {
                        Ok((_, v)) => Ok(v.value().to_vec()),
                        Err(e) => Err(e),
                    })
                    .collect();
                values.unwrap()
            }
        }
    }

    async fn commit(self) -> Result<(), ()> {
        match self {
            RedbTransaction::Read(_) => Err(()),
            RedbTransaction::Write(txn) => txn.commit().map_err(|_| ()),
        }
    }
}

pub struct SledBenchStore<'a> {
    db: &'a sled::Db,
    db_dir: &'a Path,
}

impl<'a> SledBenchStore<'a> {
    pub fn new(db: &'a sled::Db, path: &'a Path) -> Self {
        SledBenchStore { db, db_dir: path }
    }
}

impl<'a> BenchStore for SledBenchStore<'a> {
    type T<'db> = SledTransaction<'db> where Self: 'db;

    fn transaction(&self, _write: bool) -> Self::T<'_> {
        SledTransaction {
            db: self.db,
            db_dir: self.db_dir,
        }
    }
}

pub struct SledTransaction<'a> {
    db: &'a sled::Db,
    db_dir: &'a Path,
}

impl<'a> Transaction for SledTransaction<'a> {
    type Output<'out> = sled::IVec where Self: 'out;

    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        self.db.insert(key, value).map(|_| ()).map_err(|_| ())
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), ()> {
        self.db.remove(key).map(|_| ()).map_err(|_| ())
    }

    async fn commit(self) -> Result<(), ()> {
        self.db.flush().unwrap();
        // Workaround for sled durability
        // Fsync all the files, because sled doesn't guarantee durability (it uses sync_file_range())
        // See: https://github.com/spacejam/sled/issues/1351
        for entry in fs::read_dir(self.db_dir).unwrap() {
            let entry = entry.unwrap();
            if entry.path().is_file() {
                let file = File::open(entry.path()).unwrap();
                file.sync_all().unwrap();
            }
        }
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Option<sled::IVec> {
        self.db.get(key).unwrap()
    }

    fn range(&self, key: &[u8]) -> Vec<Vec<u8>> {
        let iter = self.db.range(key..);
        iter.filter_map(|x| x.ok())
            .map(|(_, v)| v.to_vec())
            .collect()
    }
}

pub struct RocksdbBenchStore<'a> {
    db: &'a TransactionDB,
}

impl<'a> RocksdbBenchStore<'a> {
    pub fn new(db: &'a TransactionDB) -> Self {
        Self { db }
    }
}

impl<'a> BenchStore for RocksdbBenchStore<'a> {
    type T<'db> = RocksdbTransaction<'db> where Self: 'db;

    fn transaction(&self, write: bool) -> Self::T<'_> {
        if write {
            let mut write_opt = WriteOptions::new();
            write_opt.set_sync(true);
            let mut txn_opt = TransactionOptions::new();
            txn_opt.set_snapshot(true);
            let txn = self.db.transaction_opt(&write_opt, &txn_opt);
            RocksdbTransaction::Write(txn)
        } else {
            let snapshot = self.db.snapshot();
            RocksdbTransaction::Read(snapshot)
        }
    }
}

pub enum RocksdbTransaction<'a> {
    Write(rocksdb::Transaction<'a, TransactionDB>),
    Read(rocksdb::SnapshotWithThreadMode<'a, TransactionDB>),
}

impl<'a> Transaction for RocksdbTransaction<'a> {
    type Output<'out> = Vec<u8> where Self: 'out;

    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        match self {
            RocksdbTransaction::Read(_) => Err(()),
            RocksdbTransaction::Write(txn) => txn.put(key, value).map_err(|_| ()),
        }
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), ()> {
        match self {
            RocksdbTransaction::Read(_) => Err(()),
            RocksdbTransaction::Write(txn) => txn.delete(key).map_err(|_| ()),
        }
    }

    async fn commit(self) -> Result<(), ()> {
        match self {
            RocksdbTransaction::Read(_) => Err(()),
            RocksdbTransaction::Write(txn) => txn.commit().map_err(|_| ()),
        }
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self {
            RocksdbTransaction::Write(_) => None,
            RocksdbTransaction::Read(snapshot) => snapshot.get(key).unwrap(),
        }
    }

    fn range(&self, key: &[u8]) -> Vec<Vec<u8>> {
        match self {
            RocksdbTransaction::Write(_) => Vec::new(),
            RocksdbTransaction::Read(snapshot) => {
                let iter = snapshot.iterator(IteratorMode::From(key, Direction::Forward));
                iter.map(|x| {
                    let x = x.unwrap();
                    x.1.to_vec()
                })
                .collect()
            }
        }
    }
}

pub struct SurrealKVBenchStore<'a> {
    db: &'a surrealkv::Store,
}

impl<'a> SurrealKVBenchStore<'a> {
    pub fn new(db: &'a surrealkv::Store) -> Self {
        SurrealKVBenchStore { db }
    }
}

impl<'a> BenchStore for SurrealKVBenchStore<'a> {
    type T<'db> = SurrealKVTransaction<'db> where Self: 'db;
    fn transaction(&self, write: bool) -> Self::T<'_> {
        if write {
            let txn = self.db.begin().unwrap();
            SurrealKVTransaction::Write(txn)
        } else {
            let txn = self.db.begin_with_mode(surrealkv::Mode::ReadOnly).unwrap();
            SurrealKVTransaction::Read(txn)
        }
    }
}

pub enum SurrealKVTransaction<'a> {
    Read(surrealkv::Transaction),
    Write(surrealkv::Transaction),
    _PhantomData(std::marker::PhantomData<&'a ()>),
}

impl<'a> Transaction for SurrealKVTransaction<'a> {
    type Output<'out> = Vec<u8> where Self: 'out;

    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        match self {
            SurrealKVTransaction::Read(_) => Err(()),
            SurrealKVTransaction::Write(txn) => txn.set(key, value).map(|_| ()).map_err(|_| ()),
            _ => unreachable!(),
        }
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), ()> {
        match self {
            SurrealKVTransaction::Read(_) => Err(()),
            SurrealKVTransaction::Write(txn) => txn.delete(key).map(|_| ()).map_err(|_| ()),
            _ => unreachable!(),
        }
    }

    async fn commit(self) -> Result<(), ()> {
        match self {
            SurrealKVTransaction::Read(_) => Err(()),
            SurrealKVTransaction::Write(mut txn) => {
                txn.commit().await.unwrap();
                Ok(())
            }
            _ => unreachable!(),
        }
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self {
            SurrealKVTransaction::Write(_) => None,
            SurrealKVTransaction::Read(txn) => txn.get(key).unwrap(),
            _ => unreachable!(),
        }
    }

    fn range(&self, key: &[u8]) -> Vec<Vec<u8>> {
        match self {
            SurrealKVTransaction::Write(_) => Vec::new(),
            SurrealKVTransaction::Read(txn) => {
                let range = key..;
                let iter = txn.scan(range, None).unwrap();
                iter.into_iter().map(|x| x.1).collect()
            }
            _ => unreachable!(),
        }
    }
}
