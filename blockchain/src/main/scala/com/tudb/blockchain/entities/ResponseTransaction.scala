package com.tudb.blockchain.entities

/**
  *@description:
  */
case class ResponseTransaction(
    from: String,
    to: String,
    token: String,
    hexStringMoney: String,
    timestamp: Long)
