package com.tudb.blockchain.eth.client

import com.alibaba.fastjson.JSONObject

/**
  *@description:
  */
class EthClientApi(host: String, port: Int) {
  private val client = new EthNodeClient(host, port)
  client.connect

  def getLatestBlockChainNumber(requestId: Int): Int = {
    client.sendJsonRequest(EthNodeJsonApi.getEthBlockNumber(requestId))
    while (client.isMessageEmpty()) {
      Thread.sleep(100)
    }
    EthJsonObjectParser.getBlockNumber(client.consumeMessage())
  }

  def getBlockInfo(blockNumber: Int): Array[EthTransaction] = {
    client.sendJsonRequest(EthNodeJsonApi.getBlockByNumber(blockNumber, true, blockNumber))
    while (client.isMessageEmpty()) {
      Thread.sleep(100)
    }
    EthJsonObjectParser.getBlockTransaction(client.consumeMessage())
  }

  def sendPullBlockDataRequest(blockNumber: Int): Unit = {
    client.sendJsonRequest(EthNodeJsonApi.getBlockByNumber(blockNumber, true, blockNumber))
  }

  def getBlockTransactions(blockJsonObject: JSONObject): Array[EthTransaction] = {
    EthJsonObjectParser.getBlockTransaction(blockJsonObject)
  }

  def consumeMessage(): JSONObject = client.consumeMessage()

  def isQueueEmpty(): Boolean = client.isMessageEmpty()

  def closeClient(): Unit = client.close()
}
