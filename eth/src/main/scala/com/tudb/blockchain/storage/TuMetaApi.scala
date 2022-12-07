package com.tudb.blockchain.storage

import com.tudb.blockchain.eth.meta.MetaKeyManager
import com.tudb.blockchain.tools.ByteUtils
import org.rocksdb.RocksDB

/**
  *@description:
  */
class TuMetaApi(db: RocksDB) {
  def setSynchronizedBlockNumber(blockNumber: Int): Unit = {
    val blockNumberByteArray = new Array[Byte](4)
    ByteUtils.setInt(blockNumberByteArray, 0, blockNumber)
    db.put(MetaKeyManager.blockNumberKey, blockNumberByteArray)
  }
  def getSynchronizedBlockNumber(): Int = {
    val res = db.get(MetaKeyManager.blockNumberKey)
    if (res == null) -1
    else ByteUtils.getInt(res, 0)
  }
}
