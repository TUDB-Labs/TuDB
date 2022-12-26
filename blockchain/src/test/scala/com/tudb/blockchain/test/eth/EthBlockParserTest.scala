package com.tudb.blockchain.test.eth

import com.tudb.blockchain.eth.{EthBlockParser, Web3jEthClient}
import org.junit.{Assert, Test}

/**
  *@description:
  */
class EthBlockParserTest {
  val client = new Web3jEthClient("https://mainnet.infura.io/v3/6afd74985a834045af3d4d8f6344730a")
  val blockParser = new EthBlockParser()
  @Test
  def testParseBlockTransactions(): Unit = {
    val latestBlockNumber = client.getEthLatestBlockNumber()
    val ethBlock = client.getEthBlockInfoByNumber(latestBlockNumber, true)
    val txs = blockParser.getBlockTransactions(ethBlock)
    txs.foreach(println)
  }
}
