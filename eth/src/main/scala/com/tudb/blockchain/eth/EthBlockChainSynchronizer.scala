package com.tudb.blockchain.eth

import com.alibaba.fastjson.JSONObject
import com.tudb.blockchain.eth.importer.TransactionImporter
import com.tudb.blockchain.eth.meta.MetaKeyManager
import com.tudb.blockchain.tools.ByteUtils
import org.rocksdb.RocksDB

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong

/**
  *@description:
  */
class EthBlockChainSynchronizer(db: RocksDB) {
  private val queue = new ConcurrentLinkedQueue[JSONObject]()
  private val client = new EthNodeClient("192.168.31.178", 8546, queue)
  client.connect

  def close(): Unit = {
    client.close()
  }

  def synchronizeBlock(): Unit = {
    val currentSynchronizedBlockNumber = {
      val blockNumber = db.get(MetaKeyManager.blockNumberKey)
      if (blockNumber == null) 0
      else ByteUtils.getInt(blockNumber, 0)
    }

    try {
      client.sendJsonRequest(EthNodeJsonApi.getEthBlockNumber(1))
      val currentBlockChainNumber: Int = EthJsonObjectParser.getBlockNumber(client.consumeResult())

      for (blockNumber <- currentSynchronizedBlockNumber + 1 to currentBlockChainNumber) {
        client.sendJsonRequest(EthNodeJsonApi.getBlockByNumber(blockNumber, true, blockNumber))
      }

      val txCount = new AtomicLong(0)
      val txImporter = new TransactionImporter(db, queue, txCount)

      while (!queue.isEmpty) {
        txImporter.importer()
      }

      // update meta
      val arrayBytes: Array[Byte] = new Array[Byte](4)
      ByteUtils.setInt(arrayBytes, 0, currentBlockChainNumber)
      db.put(MetaKeyManager.blockNumberKey, arrayBytes)

    } catch {
      case e: Exception => {
        println(s"client error...${e.getMessage}")
      }
    }
  }
}
