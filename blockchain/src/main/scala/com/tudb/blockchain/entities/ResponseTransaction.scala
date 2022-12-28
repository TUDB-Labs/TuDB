package com.tudb.blockchain.entities

import java.math.BigInteger

/**
  *@description:
  */
case class ResponseTransaction(
    from: String,
    to: String,
    token: String,
    money: BigInteger,
    timestamp: Long)
