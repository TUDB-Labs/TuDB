package com.tudb.blockchain.eth

import com.tudb.blockchain.eth.entity.{EthTransactionKey, EthTransactionKeyBytes}
import com.tudb.tools.{ByteUtils, HexStringUtils}
import com.tudb.tools.HexStringUtils.{IN_TX_TYPE, OUT_TX_TYPE, hexString2ArrayBytes, removeHexStringHeader}

/**
  *@description:
  */
object EthKeyConverter {

//  def toAddressKey(
//      address: String,
//      label: Byte = LABEL_TYPE,
//      chainType: Byte = CHAIN_TYPE,
//      tokenType: Byte = TOKEN_TYPE
//    ): (Array[Byte], Array[Byte]) = {
//    val hexBytes = hexString2ArrayBytes(address)
//
//    (
//      Array[Byte](ADDRESS_LABEL_TYPE, chainType, tokenType) ++ hexBytes ++ Array[Byte](label),
//      Array[Byte](LABEL_ADDRESS_TYPE, chainType, tokenType, label) ++ hexBytes
//    )
//  }

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

    val outKey = Array[Byte](OUT_TX_TYPE) ++ tokenBytes ++ fromHexBytes ++ toHexBytes ++ timestampBytes ++ TxHashBytes
    val inKey = Array[Byte](IN_TX_TYPE) ++ tokenBytes ++ toHexBytes ++ fromHexBytes ++ timestampBytes ++ TxHashBytes

    EthTransactionKeyBytes(outKey, inKey)
  }

  def outTransactionKeyToEntity(key: Array[Byte]): EthTransactionKey = {
    val direction: String = new String(key.slice(0, 1))
    val tokenId = ByteUtils.getInt(key.slice(1, 5), 0)
    val from = HexStringUtils.arrayBytes2HexString(key.slice(5, 25))
    val to = HexStringUtils.arrayBytes2HexString(key.slice(25, 45))
    val timestamp = ~ByteUtils.getLong(key.slice(45, 53), 0)
    val txHash = HexStringUtils.arrayBytes2HexString(key.drop(53))
    EthTransactionKey(direction, tokenId, from, to, timestamp, txHash)
  }
  def inTransactionKeyToEntity(key: Array[Byte]): EthTransactionKey = {
    val direction: String = new String(key.slice(0, 1))
    val tokenId = ByteUtils.getInt(key.slice(1, 5), 0)
    val to = HexStringUtils.arrayBytes2HexString(key.slice(5, 25))
    val from = HexStringUtils.arrayBytes2HexString(key.slice(25, 45))
    val timestamp = ~ByteUtils.getLong(key.slice(45, 53), 0)
    val txHash = HexStringUtils.arrayBytes2HexString(key.drop(53))
    EthTransactionKey(direction, tokenId, from, to, timestamp, txHash)
  }
}
