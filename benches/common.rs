// This code is borrowed from: https://github.com/cberner/redb/blob/master/benches/common.rs
//
// Copyright (c) 2021 Christopher Berner
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use redb::ReadableTable;
use redb::TableDefinition;
use rocksdb::{Direction, IteratorMode, TransactionDB, TransactionOptions, WriteOptions};
use std::fs;
use std::fs::File;
use std::path::Path;

const X: TableDefinition<&[u8], &[u8]> = TableDefinition::new("x");

pub(crate) trait BenchDatabase {
    type W<'db>: BenchWriteTransaction
    where
        Self: 'db;
    type R<'db>: BenchReadTransaction
    where
        Self: 'db;

    fn store_name() -> &'static str;
    fn write_transaction(&self) -> Self::W<'_>;
    fn read_transaction(&self) -> Self::R<'_>;
}

pub(crate) trait BenchWriteTransaction {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()>;
    fn remove(&mut self, key: &[u8]) -> Result<(), ()>;
    async fn commit(self) -> Result<(), ()>;
}

pub trait BenchReadTransaction {
    type Output<'out>: AsRef<[u8]> + 'out
    where
        Self: 'out;

    fn get<'a>(&'a self, key: &[u8]) -> Option<Self::Output<'a>>;
    fn range(&self, key: &[u8]) -> Vec<Vec<u8>>;
}

pub struct RedbBenchDatabase<'a> {
    db: &'a redb::Database,
}

impl<'a> RedbBenchDatabase<'a> {
    #[allow(dead_code)]
    pub fn new(db: &'a redb::Database) -> Self {
        RedbBenchDatabase { db }
    }
}

impl<'a> BenchDatabase for RedbBenchDatabase<'a> {
    type W<'db> = RedbBenchWriteTransaction<'db> where Self: 'db;
    type R<'db> = RedbBenchReadTransaction<'db> where Self: 'db;

    fn store_name() -> &'static str {
        "redb"
    }

    fn write_transaction(&self) -> Self::W<'_> {
        let txn = self.db.begin_write().unwrap();
        RedbBenchWriteTransaction { txn }
    }

    fn read_transaction(&self) -> Self::R<'_> {
        let txn = self.db.begin_read().unwrap();
        RedbBenchReadTransaction { txn }
    }
}

pub struct RedbBenchReadTransaction<'a> {
    txn: redb::ReadTransaction<'a>,
}

impl<'a> BenchReadTransaction for RedbBenchReadTransaction<'a> {
    type Output<'out> = Vec<u8> where Self: 'out;

    fn get<'b>(&'b self, key: &[u8]) -> Option<Self::Output<'b>> {
        let table = self.txn.open_table(X).unwrap();
        let res = table.get(key).unwrap();
        res.map(|value| value.value().to_vec())
    }

    fn range(&self, key: &[u8]) -> Vec<Vec<u8>> {
        let table = self.txn.open_table(X).unwrap();
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

pub struct RedbBenchWriteTransaction<'a> {
    txn: redb::WriteTransaction<'a>,
}

impl<'a> BenchWriteTransaction for RedbBenchWriteTransaction<'a> {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        let mut table = self.txn.open_table(X).unwrap();
        table.insert(key, value).map(|_| ()).map_err(|_| ())
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), ()> {
        let mut table = self.txn.open_table(X).unwrap();
        table.remove(key).map(|_| ()).map_err(|_| ())
    }

    async fn commit(self) -> Result<(), ()> {
        self.txn.commit().map_err(|_| ())
    }
}

pub struct SledBenchDatabase<'a> {
    db: &'a sled::Db,
    db_dir: &'a Path,
}

impl<'a> SledBenchDatabase<'a> {
    pub fn new(db: &'a sled::Db, path: &'a Path) -> Self {
        SledBenchDatabase { db, db_dir: path }
    }
}

impl<'a> BenchDatabase for SledBenchDatabase<'a> {
    type W<'db> = SledBenchWriteTransaction<'db> where Self: 'db;
    type R<'db> = SledBenchReadTransaction<'db> where Self: 'db;

    fn store_name() -> &'static str {
        "sled"
    }

    fn write_transaction(&self) -> Self::W<'_> {
        SledBenchWriteTransaction {
            db: self.db,
            db_dir: self.db_dir,
        }
    }

    fn read_transaction(&self) -> Self::R<'_> {
        SledBenchReadTransaction { db: self.db }
    }
}

pub struct SledBenchReadTransaction<'db> {
    db: &'db sled::Db,
}

impl<'db> BenchReadTransaction for SledBenchReadTransaction<'db> {
    type Output<'out> = sled::IVec where Self: 'out;

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

pub struct SledBenchWriteTransaction<'a> {
    db: &'a sled::Db,
    db_dir: &'a Path,
}

impl<'a> BenchWriteTransaction for SledBenchWriteTransaction<'a> {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        self.db.insert(key, value).map(|_| ()).map_err(|_| ())
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), ()> {
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
}

pub struct RocksdbBenchDatabase<'a> {
    db: &'a TransactionDB,
}

impl<'a> RocksdbBenchDatabase<'a> {
    pub fn new(db: &'a TransactionDB) -> Self {
        Self { db }
    }
}

impl<'a> BenchDatabase for RocksdbBenchDatabase<'a> {
    type W<'db> = RocksdbBenchWriteTransaction<'db> where Self: 'db;
    type R<'db> = RocksdbBenchReadTransaction<'db> where Self: 'db;

    fn store_name() -> &'static str {
        "rocksdb"
    }

    fn write_transaction(&self) -> Self::W<'_> {
        let mut write_opt = WriteOptions::new();
        write_opt.set_sync(true);
        let mut txn_opt = TransactionOptions::new();
        txn_opt.set_snapshot(true);
        let txn = self.db.transaction_opt(&write_opt, &txn_opt);
        RocksdbBenchWriteTransaction { txn }
    }

    fn read_transaction(&self) -> Self::R<'_> {
        let snapshot = self.db.snapshot();
        RocksdbBenchReadTransaction { snapshot }
    }
}

pub struct RocksdbBenchWriteTransaction<'a> {
    txn: rocksdb::Transaction<'a, TransactionDB>,
}

impl<'a> BenchWriteTransaction for RocksdbBenchWriteTransaction<'a> {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        self.txn.put(key, value).map_err(|_| ())
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), ()> {
        self.txn.delete(key).map_err(|_| ())
    }

    async fn commit(self) -> Result<(), ()> {
        self.txn.commit().map_err(|_| ())
    }
}

pub struct RocksdbBenchReadTransaction<'db> {
    snapshot: rocksdb::SnapshotWithThreadMode<'db, TransactionDB>,
}

impl<'db> BenchReadTransaction for RocksdbBenchReadTransaction<'db> {
    type Output<'out> = Vec<u8> where Self: 'out;

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.snapshot.get(key).unwrap()
    }

    fn range(&self, key: &[u8]) -> Vec<Vec<u8>> {
        let iter = self
            .snapshot
            .iterator(IteratorMode::From(key, Direction::Forward));
        iter.map(|x| {
            let x = x.unwrap();
            x.1.to_vec()
        })
        .collect()
    }
}

pub struct SurrealKVBenchDatabase<'a> {
    db: &'a surrealkv::Store,
}

impl<'a> SurrealKVBenchDatabase<'a> {
    pub fn new(db: &'a surrealkv::Store) -> Self {
        SurrealKVBenchDatabase { db }
    }
}

impl<'a> BenchDatabase for SurrealKVBenchDatabase<'a> {
    type W<'db> = SurrealKVBenchWriteTransaction where Self: 'db;
    type R<'db> = SurrealKVBenchReadTransaction where Self: 'db;

    fn store_name() -> &'static str {
        "surrealkv"
    }

    fn write_transaction(&self) -> Self::W<'_> {
        let txn = self.db.begin().unwrap();
        SurrealKVBenchWriteTransaction { txn }
    }

    fn read_transaction(&self) -> Self::R<'_> {
        let txn = self.db.begin().unwrap();
        SurrealKVBenchReadTransaction { txn }
    }
}

pub struct SurrealKVBenchReadTransaction {
    txn: surrealkv::Transaction,
}

impl BenchReadTransaction for SurrealKVBenchReadTransaction {
    type Output<'out> = Vec<u8> where Self: 'out;

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.txn.get(key).unwrap()
    }

    fn range(&self, key: &[u8]) -> Vec<Vec<u8>> {
        let range = key..;
        let iter = self.txn.scan(range, None).unwrap();
        iter.into_iter().map(|x| x.1).collect()
    }
}

pub struct SurrealKVBenchWriteTransaction {
    txn: surrealkv::Transaction,
}

impl BenchWriteTransaction for SurrealKVBenchWriteTransaction {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        self.txn.set(key, value).map(|_| ()).map_err(|_| ())
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), ()> {
        self.txn.delete(key).map(|_| ()).map_err(|_| ())
    }

    async fn commit(mut self) -> Result<(), ()> {
        self.txn.commit().await.unwrap();

        Ok(())
    }
}
