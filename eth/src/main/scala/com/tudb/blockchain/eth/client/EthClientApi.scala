package com.tudb.blockchain.eth.client

import com.alibaba.fastjson.JSONObject

/**
  *@description:
  */
class EthClientApi(host: String, port: Int) {
  private val client = new EthNodeClient(host, port)
  client.connect

  def getBlockChainNumber(): Int = {
    client.sendJsonRequest(EthNodeJsonApi.getEthBlockNumber(1))
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

  def getJsonObject(): JSONObject = client.consumeMessage()

  def isQueueEmpty(): Boolean = client.isMessageEmpty()

  def closeClient(): Unit = client.close()
}
