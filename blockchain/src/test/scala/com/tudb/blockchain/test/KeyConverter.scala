package com.tudb.blockchain.test

import com.tudb.blockchain.converter.BlockchainKeyConverter
import org.junit.Test

/**
  *@description:
  */
class KeyConverter {

  @Test
  def testKeyConverter(): Unit = {
    val tokenId = 233
    val from = "0xe5249bf42bc2b5f8bf8772b5aeaaed79322431ca"
    val to = "0x4afd163f281ed126cb07eaf99f52d8a083e135e3"
    val timestamp = 1672123307L
    val txHash = "0x79d9bad8c523c4056c8d04a321a43657e1b9989175ebdb6b4e06b2017e57dc18"

    val key = BlockchainKeyConverter.toTransactionKeyBytes(tokenId, from, to, timestamp, txHash)

  }
}
