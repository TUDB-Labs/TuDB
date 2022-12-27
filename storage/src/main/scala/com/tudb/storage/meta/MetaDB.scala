package com.tudb.storage.meta

import com.tudb.storage.RocksDBStorageConfig
import org.rocksdb.RocksDB

/**
  *@description:
  */
class MetaDB(dbPath: String) {
  val db = RocksDB.open(RocksDBStorageConfig.getDefaultOptions(true), dbPath)

}
