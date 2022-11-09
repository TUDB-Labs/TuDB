package org.demo.eth.eth

/**
  *@description:
  */
object EthJsonApi {

  def getEthAccountBalance(address: String, msgId: Int): String = {
    val hexAddress = "0x" + address
    s"""{"jsonrpc":"2.0","method":"eth_getBalance","params":["$hexAddress", "latest"],"id":$msgId}"""
  }

  def getEthBlockNumber(msgId: Int): String = {
    s"""{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":$msgId}"""
  }

  def getBlockByNumber(blockNumber: Int, fullTxObject: Boolean, msgId: Int): String = {
    val hexIndex = "0x" + Integer.toHexString(blockNumber)
    s"""{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["$hexIndex", $fullTxObject],"id":$msgId}"""
  }
}
