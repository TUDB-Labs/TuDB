package com.tudb.blockchain.storage

import com.tudb.blockchain.eth.EthKeyConverter
import org.rocksdb.{ReadOptions, RocksDB}

/**
  *@description:
  */
class InnerQueryApi(db: RocksDB) {

  def innerFindOutKey(fromAddress: Array[Byte]): Iterator[(Array[Byte], Array[Byte])] = {
    val prefix = Array[Byte](
      EthKeyConverter.OUT_TX_TYPE,
      EthKeyConverter.CHAIN_TYPE,
      EthKeyConverter.TOKEN_TYPE
    ) ++ fromAddress
    new EthTransactionPrefixIterator(prefix, db)
  }

  def innerFindInKey(toAddress: Array[Byte]): Iterator[(Array[Byte], Array[Byte])] = {
    val prefix = Array[Byte](
      EthKeyConverter.IN_TX_TYPE,
      EthKeyConverter.CHAIN_TYPE,
      EthKeyConverter.TOKEN_TYPE
    ) ++ toAddress
    new EthTransactionPrefixIterator(prefix, db)
  }

  def innerGetAllAddresses(): Iterator[Array[Byte]] = {
    val prefix = Array[Byte](EthKeyConverter.ADDRESS_LABEL_TYPE)
    new EthAddressPrefixIterator(prefix, db)
  }
}

class EthTransactionPrefixIterator(prefix: Array[Byte], db: RocksDB)
  extends Iterator[(Array[Byte], Array[Byte])] {
  val readOptions = new ReadOptions()
  val iter = db.newIterator()
  iter.seek(prefix)

  override def hasNext: Boolean = iter.isValid && iter.key().startsWith(prefix)

  override def next(): (Array[Byte], Array[Byte]) = {
    val address = iter.key()
    val wei = iter.value()
    iter.next()
    (address, wei)
  }
}

class EthAddressPrefixIterator(prefix: Array[Byte], db: RocksDB) extends Iterator[Array[Byte]] {
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
