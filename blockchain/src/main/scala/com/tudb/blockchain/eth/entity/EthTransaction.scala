package com.tudb.blockchain.eth.entity

/**
  *@description:
  */
case class EthTransaction(
    from: String,
    to: String,
    money: String,
    timestamp: Long,
    txHash: String) {}
