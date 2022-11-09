package org.demo.eth

import org.demo.eth.eth.EthKeyConverter
import org.rocksdb.RocksDB

/**
  *@description:
  */
class QueryApi(db: RocksDB) {
  def innerFindOutAddressesOfTransactions(fromAddress: Array[Byte]): Iterator[Array[Byte]] = {
    val prefix = Array[Byte](
      EthKeyConverter.OUT_TX_TYPE,
      EthKeyConverter.CHAIN_TYPE,
      EthKeyConverter.TOKEN_TYPE
    ) ++ fromAddress
    new EthPrefixIterator(prefix, db).map(key => key.slice(23, 43))
  }

  def innerGetAllAddresses(): Iterator[Array[Byte]] = {
    val prefix = Array[Byte](EthKeyConverter.ADDRESS_LABEL_TYPE)
    new EthPrefixIterator(prefix, db).map(key => key.slice(3, 23))
  }

  def hop1Query(address: Array[Byte]): Iterator[Array[Byte]] = {
    innerFindOutAddressesOfTransactions(address)
  }

  def hop2Query(address: Array[Byte]): Iterator[Array[Byte]] = {
    innerFindOutAddressesOfTransactions(address).flatMap(hop1Address => {
      innerFindOutAddressesOfTransactions(hop1Address)
    })
  }

  def hop3Query(address: Array[Byte]): Iterator[Array[Byte]] = {
    innerFindOutAddressesOfTransactions(address).flatMap(hop1Address => {
      innerFindOutAddressesOfTransactions(hop1Address).flatMap(hop2Address => {
        innerFindOutAddressesOfTransactions(hop2Address)
      })
    })
  }
}

class EthPrefixIterator(prefix: Array[Byte], db: RocksDB) extends Iterator[Array[Byte]] {
  val iter = db.newIterator()
  iter.seek(prefix)

  override def hasNext: Boolean = iter.isValid && iter.key().startsWith(prefix)

  override def next(): Array[Byte] = {
    val key = iter.key()
    iter.next()
    key
  }
}
