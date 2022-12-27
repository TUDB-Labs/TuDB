package com.tudb.blockchain.eth.entity

/**
  *@description:
  */
case class EthTransactionKey(
    direction: String,
    tokenId: Int,
    from: String,
    to: String,
    timestamp: Long,
    txHash: String)
