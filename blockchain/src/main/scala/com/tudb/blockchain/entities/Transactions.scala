package com.tudb.blockchain.entities

/**
  *@description:
  */
trait TransactionWithFullInfo {
  val from: String
  val to: String
  val tokenName: String
  val nativeHexStringMoney: String
  val timestamp: Long
  val txHash: String
}

case class EthTransaction(
    from: String,
    to: String,
    tokenName: String,
    nativeHexStringMoney: String,
    timestamp: Long,
    txHash: String)
  extends TransactionWithFullInfo {}
