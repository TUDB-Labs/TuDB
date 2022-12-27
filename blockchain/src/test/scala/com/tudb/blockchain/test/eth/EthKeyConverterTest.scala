package com.tudb.blockchain.test.eth

import com.tudb.blockchain.eth.entity.EthTransactionKey
import com.tudb.blockchain.eth.EthKeyConverter
import com.tudb.tools.HexStringUtils
import org.junit.{Assert, Test}

/**
  *@description:
  */
class EthKeyConverterTest {
  @Test
  def testKeyConverter(): Unit = {
    val tokenId = 233
    val from = "0xe5249bf42bc2b5f8bf8772b5aeaaed79322431ca"
    val to = "0x4afd163f281ed126cb07eaf99f52d8a083e135e3"
    val money = "1ddc0eb2154400"
    val timestamp = 1672123307L
    val txHash = "0x79d9bad8c523c4056c8d04a321a43657e1b9989175ebdb6b4e06b2017e57dc18"

    val key = EthKeyConverter.toTransactionKeyBytes(tokenId, from, to, timestamp, txHash)
    val outEntity = EthKeyConverter.outTransactionKeyToEntity(key.outKey)
    val inEntity = EthKeyConverter.inTransactionKeyToEntity(key.inKey)

    val relOutEntity = EthTransactionKey(
      "o",
      tokenId,
      HexStringUtils.removeHexStringHeader(from),
      HexStringUtils.removeHexStringHeader(to),
      timestamp,
      HexStringUtils.removeHexStringHeader(txHash)
    )
    val relInEntity = EthTransactionKey(
      "i",
      tokenId,
      HexStringUtils.removeHexStringHeader(from),
      HexStringUtils.removeHexStringHeader(to),
      timestamp,
      HexStringUtils.removeHexStringHeader(txHash)
    )

    Assert.assertEquals(relOutEntity, outEntity)
    Assert.assertEquals(relInEntity, inEntity)
  }
}
