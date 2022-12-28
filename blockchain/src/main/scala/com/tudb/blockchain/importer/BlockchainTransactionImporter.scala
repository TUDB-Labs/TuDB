package com.tudb.blockchain.importer

import com.tudb.blockchain.converter.BlockchainKeyConverter
import com.tudb.blockchain.entities.TransactionWithFullInfo
import com.tudb.storage.RocksDBStorageConfig
import com.tudb.storage.meta.MetaStoreApi
import com.tudb.tools.HexStringUtils
import org.rocksdb.{RocksDB, WriteBatch, WriteOptions}

import scala.collection.mutable.ArrayBuffer

/**
  *@description:
  */
class BlockchainTransactionImporter(dbPath: String, blockchain: String) {
  val chainDBPath = s"${dbPath}/${blockchain}.db"
  val metaDBPath = s"${dbPath}/meta.db"

  val chainDB: RocksDB = RocksDB.open(RocksDBStorageConfig.getDefaultOptions(true), chainDBPath)
  val metaDB: RocksDB = RocksDB.open(RocksDBStorageConfig.getDefaultOptions(true), metaDBPath)
  val metaStoreApi = new MetaStoreApi(metaDB)

  // rocksdb
  val writeOptions = new WriteOptions()
  writeOptions.setDisableWAL(false)
  val writeBatch = new WriteBatch()

  // key, money
  val outTxArray = ArrayBuffer[(Array[Byte], Array[Byte])]()
  val inTxArray = ArrayBuffer[(Array[Byte], Array[Byte])]()

  def importer(txs: Seq[TransactionWithFullInfo]): Unit = {
    txs.foreach(tx => {
      val fromAddress = HexStringUtils.removeHexStringHeader(tx.from)
      val toAddress = HexStringUtils.removeHexStringHeader(tx.to)
      val money = HexStringUtils.removeHexStringHeader(tx.nativeHexStringMoney)
      val timestamp = ~tx.timestamp // negation long
      val txHash = HexStringUtils.removeHexStringHeader(tx.txHash)

      val key =
        BlockchainKeyConverter.toTransactionKeyBytes(
          metaStoreApi.getOrAddTokenName(tx.tokenName),
          fromAddress,
          toAddress,
          timestamp,
          txHash
        )

      val value = HexStringUtils.hexString2ArrayBytes(money)

      outTxArray.append((key.outKey, value))
      inTxArray.append((key.inKey, value))
    })

    outTxArray.foreach(kv => writeBatch.put(kv._1, kv._2))
    inTxArray.foreach(kv => writeBatch.put(kv._1, kv._2))

    outTxArray.clear()
    inTxArray.clear()

    chainDB.write(writeOptions, writeBatch)
    writeBatch.clear()
  }

  def close(): Unit = {
    chainDB.close()
    metaDB.close()
  }

}
