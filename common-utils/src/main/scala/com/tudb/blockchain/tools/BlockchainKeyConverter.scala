package com.tudb.blockchain.tools

import HexStringUtils.{IN_TX_TYPE, OUT_TX_TYPE, hexString2ArrayBytes, removeHexStringHeader}

/**
  *@description:
  */
object BlockchainKeyConverter {

  def toTransactionKeyBytes(
      tokenId: Int,
      fromAddress: String,
      toAddress: String,
      timestamp: Long,
      txHash: String
    ): InnerTransactionKeyBytes = {
    val fromHexBytes = hexString2ArrayBytes(removeHexStringHeader(fromAddress))
    val toHexBytes = hexString2ArrayBytes(removeHexStringHeader(toAddress))
    val TxHashBytes = hexString2ArrayBytes(removeHexStringHeader(txHash))

    val tokenBytes = new Array[Byte](4)
    ByteUtils.setInt(tokenBytes, 0, tokenId)

    val timestampBytes = new Array[Byte](8)
    ByteUtils.setLong(timestampBytes, 0, timestamp)

    val outKey = Array[Byte](OUT_TX_TYPE) ++ fromHexBytes ++ toHexBytes ++ timestampBytes ++ tokenBytes ++ TxHashBytes
    val inKey = Array[Byte](IN_TX_TYPE) ++ toHexBytes ++ fromHexBytes ++ timestampBytes ++ tokenBytes ++ TxHashBytes

    InnerTransactionKeyBytes(outKey, inKey)
  }
}
