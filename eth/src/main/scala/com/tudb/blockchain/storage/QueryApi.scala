package com.tudb.blockchain.storage

import com.tudb.blockchain.eth.EthKeyConverter
import org.rocksdb.{ReadOptions, RocksDB}

/**
  *@description:
  */
class QueryApi(db: RocksDB) {

  def innerFindOutKey(fromAddress: Array[Byte]): Iterator[Array[Byte]] = {
    val prefix = Array[Byte](
      EthKeyConverter.OUT_TX_TYPE,
      EthKeyConverter.CHAIN_TYPE,
      EthKeyConverter.TOKEN_TYPE
    ) ++ fromAddress
    new EthPrefixIterator(prefix, db)
  }
  def innerFindInKey(toAddress: Array[Byte]): Iterator[Array[Byte]] = {
    val prefix = Array[Byte](
      EthKeyConverter.IN_TX_TYPE,
      EthKeyConverter.CHAIN_TYPE,
      EthKeyConverter.TOKEN_TYPE
    ) ++ toAddress
    new EthPrefixIterator(prefix, db)
  }

  def innerFindOutAddressAndTxHash(
      fromAddress: Array[Byte]
    ): Iterator[(Array[Byte], Array[Byte])] = {
    innerFindOutKey(fromAddress).map(key => (key.slice(23, 43), key.slice(51, 83)))
  }

  def innerFindOutAddressesOfTransactions(fromAddress: Array[Byte]): Iterator[Array[Byte]] = {
    innerFindOutKey(fromAddress).map(key => key.slice(23, 43))
  }
  def innerFindInAddressesOfTransactions(toAddress: Array[Byte]): Iterator[Array[Byte]] = {
    innerFindInKey(toAddress).map(key => key.slice(23, 43))
  }

  def innerGetAllAddresses(): Iterator[Array[Byte]] = {
    val prefix = Array[Byte](EthKeyConverter.ADDRESS_LABEL_TYPE)
    new EthPrefixIterator(prefix, db).map(key => key.slice(3, 23))
  }

  def hop1Query(address: Array[Byte]): Iterator[Array[Byte]] = {
    innerFindOutAddressesOfTransactions(address)
  }

  def hop2Query(address: Array[Byte]): Iterator[Array[Byte]] = {
    // A to B can happen multiple times, but next hop query we only need query once
    innerFindOutAddressesOfTransactions(address).toSeq.distinct
      .flatMap(hop1Address => {
        innerFindOutAddressesOfTransactions(hop1Address)
      })
      .toIterator
  }

  def hop3Query(address: Array[Byte]): Iterator[Array[Byte]] = {
    // A to B can happen multiple times, but next hop query we only need query once
    innerFindOutAddressesOfTransactions(address).toSeq.distinct
      .flatMap(hop1Address => {
        innerFindOutAddressesOfTransactions(hop1Address).toSeq.distinct.flatMap(hop2Address => {
          innerFindOutAddressesOfTransactions(hop2Address)
        })
      })
      .iterator
  }
}

class EthPrefixIterator(prefix: Array[Byte], db: RocksDB) extends Iterator[Array[Byte]] {
  val readOptions = new ReadOptions()
  val iter = db.newIterator()
  iter.seek(prefix)

  override def hasNext: Boolean = iter.isValid && iter.key().startsWith(prefix)

  override def next(): Array[Byte] = {
    val key = iter.key()
    iter.next()
    key
  }
}
