package com.tudb.blockchain.eth

import org.web3j.protocol.Web3j
import org.web3j.protocol.core.DefaultBlockParameter
import org.web3j.protocol.core.methods.response.{EthBlock, Transaction}
import org.web3j.protocol.http.HttpService

import java.math.BigInteger
import java.util.Optional

/**
  *@description:
  */
class Web3jEthClient(url: String) {
  private val client = Web3j.build(new HttpService(url))

  def getEthTransactionByHash(hash: String): Optional[Transaction] = {
    client.ethGetTransactionByHash(hash).send().getTransaction
  }

  def getEthLatestBlockNumber(): BigInteger = {
    client.ethBlockNumber().send().getBlockNumber
  }

  def getEthBlockInfoByNumber(
      blockNumber: BigInteger,
      withFullTransaction: Boolean
    ): EthBlock.Block = {
    client
      .ethGetBlockByNumber(DefaultBlockParameter.valueOf(blockNumber), withFullTransaction)
      .send()
      .getBlock
  }

}
