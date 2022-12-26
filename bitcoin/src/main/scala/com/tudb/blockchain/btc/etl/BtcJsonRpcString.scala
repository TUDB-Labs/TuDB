package com.tudb.blockchain.btc.etl

/**
  *@description:
  */
object BtcJsonRpcString {

  def getBlockHash(blockNumber: Int): String = {
    s"""{"jsonrpc": "2.0", "id": $blockNumber, "method": "getblockhash", "params": [$blockNumber]}"""
  }

  def getBlock(blockHash: String, verbosity: Int): String = {
    s"""{"jsonrpc": "2.0", "id": 0, "method": "getblock", "params": ["$blockHash", $verbosity]}"""
  }

  def getBlockCount(): String = {
    s"""{"jsonrpc": "2.0", "id": 0, "method": "getblockcount", "params": []}"""
  }

  def getTransaction(txid: String, verbosity: Boolean): String = {
    s"""{"jsonrpc": "2.0", "id": 0, "method": "getrawtransaction", "params": ["$txid", $verbosity]}"""
  }
}
