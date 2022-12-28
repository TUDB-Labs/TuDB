package com.tudb.blockchain.eth.entity

import java.math.BigInteger

/**
  *@description:
  */
case class EthTransaction(
    from: String,
    to: String,
    token: String,
    money: String,
    timestamp: Long,
    txHash: String) {}

case class ResponseTransaction(
    from: String,
    to: String,
    token: String,
    money: BigInteger,
    timestamp: Long)
