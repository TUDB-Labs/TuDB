package com.tudb.blockchain.eth

import com.tudb.blockchain.eth.entity.{EthTransactionKey, EthTransactionKeyBytes}
import com.tudb.tools.{ByteUtils, HexStringUtils}
import com.tudb.tools.HexStringUtils.{IN_TX_TYPE, OUT_TX_TYPE, hexString2ArrayBytes, removeHexStringHeader}

/**
  *@description:
  */
object EthKeyConverter {

  def toTransactionKeyBytes(
      tokenId: Int,
      fromAddress: String,
      toAddress: String,
      timestamp: Long,
      txHash: String
    ): EthTransactionKeyBytes = {
    val fromHexBytes = hexString2ArrayBytes(removeHexStringHeader(fromAddress))
    val toHexBytes = hexString2ArrayBytes(removeHexStringHeader(toAddress))
    val TxHashBytes = hexString2ArrayBytes(removeHexStringHeader(txHash))

    val tokenBytes = new Array[Byte](4)
    ByteUtils.setInt(tokenBytes, 0, tokenId)

    val timestampBytes = new Array[Byte](8)
    ByteUtils.setLong(timestampBytes, 0, timestamp)

    val outKey = Array[Byte](OUT_TX_TYPE) ++ fromHexBytes ++ toHexBytes ++ timestampBytes ++ tokenBytes ++ TxHashBytes
    val inKey = Array[Byte](IN_TX_TYPE) ++ toHexBytes ++ fromHexBytes ++ timestampBytes ++ tokenBytes ++ TxHashBytes

    EthTransactionKeyBytes(outKey, inKey)
  }

  def outTransactionKeyToEntity(key: Array[Byte]): EthTransactionKey = {
    val direction: String = new String(key.slice(0, 1))
    val from = HexStringUtils.arrayBytes2HexString(key.slice(1, 21))
    val to = HexStringUtils.arrayBytes2HexString(key.slice(21, 41))
    val timestamp = ~ByteUtils.getLong(key.slice(41, 49), 0)
    val tokenId = ByteUtils.getInt(key.slice(49, 53), 0)
    val txHash = HexStringUtils.arrayBytes2HexString(key.drop(53))
    EthTransactionKey(direction, tokenId, from, to, timestamp, txHash)
  }
  def inTransactionKeyToEntity(key: Array[Byte]): EthTransactionKey = {
    val direction: String = new String(key.slice(0, 1))
    val to = HexStringUtils.arrayBytes2HexString(key.slice(1, 21))
    val from = HexStringUtils.arrayBytes2HexString(key.slice(21, 41))
    val timestamp = ~ByteUtils.getLong(key.slice(41, 49), 0)
    val tokenId = ByteUtils.getInt(key.slice(49, 53), 0)
    val txHash = HexStringUtils.arrayBytes2HexString(key.drop(53))
    EthTransactionKey(direction, tokenId, from, to, timestamp, txHash)
  }
}
