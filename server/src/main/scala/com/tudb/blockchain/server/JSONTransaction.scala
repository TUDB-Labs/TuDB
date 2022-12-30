package com.tudb.blockchain.server

/**
  *@description:
  */
case class JSONTransaction(
    from: String,
    to: String,
    timestamp: Long,
    token: String,
    money: String) {}
