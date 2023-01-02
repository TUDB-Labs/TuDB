package com.tudb.blockchain.test

import com.tudb.blockchain.tools.{BlockchainKeyConverter, ByteUtils, HexStringUtils}
import org.junit.{Assert, Test}

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
    val outKey = key.outKey
    val inKey = key.inKey

    var deserializeFrom = "0x" + HexStringUtils.arrayBytes2HexString(outKey.slice(1, 21))
    var deserializeTo = "0x" + HexStringUtils.arrayBytes2HexString(outKey.slice(21, 41))
    var deserializeTimestamp = ByteUtils.getLong(outKey.slice(41, 49), 0)
    var deserializeToken = ByteUtils.getInt(outKey.slice(49, 53), 0)
    var deserializeHash = "0x" + HexStringUtils.arrayBytes2HexString(outKey.drop(53))

    Assert.assertEquals(from, deserializeFrom)
    Assert.assertEquals(to, deserializeTo)
    Assert.assertEquals(timestamp, deserializeTimestamp)
    Assert.assertEquals(tokenId, deserializeToken)
    Assert.assertEquals(txHash, deserializeHash)

    deserializeTo = "0x" + HexStringUtils.arrayBytes2HexString(inKey.slice(1, 21))
    deserializeFrom = "0x" + HexStringUtils.arrayBytes2HexString(inKey.slice(21, 41))
    deserializeTimestamp = ByteUtils.getLong(inKey.slice(41, 49), 0)
    deserializeToken = ByteUtils.getInt(inKey.slice(49, 53), 0)
    deserializeHash = "0x" + HexStringUtils.arrayBytes2HexString(inKey.drop(53))

    Assert.assertEquals(from, deserializeFrom)
    Assert.assertEquals(to, deserializeTo)
    Assert.assertEquals(timestamp, deserializeTimestamp)
    Assert.assertEquals(tokenId, deserializeToken)
    Assert.assertEquals(txHash, deserializeHash)

  }
}
