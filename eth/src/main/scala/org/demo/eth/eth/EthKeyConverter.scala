package org.demo.eth.eth

import org.demo.eth.tools.EthTools.hexString2ArrayBytes

/**
  *@description:
  */
object EthKeyConverter {
  val CHAIN_TYPE: Byte = 1.toByte // eth
  val TOKEN_TYPE: Byte = 1.toByte // etc
  val LABEL_TYPE: Byte = 1.toByte // NORMAL

  val ADDRESS_LABEL_TYPE: Byte = 'a'.toByte
  val LABEL_ADDRESS_TYPE: Byte = 'A'.toByte

  val OUT_TX_TYPE: Byte = 'r'.toByte
  val IN_TX_TYPE: Byte = 'R'.toByte

  def toAddressKey(
      address: String,
      label: Byte = LABEL_TYPE,
      chainType: Byte = CHAIN_TYPE,
      tokenType: Byte = TOKEN_TYPE
    ): (Array[Byte], Array[Byte]) = {
    val hexBytes = hexString2ArrayBytes(address)

    (
      Array[Byte](ADDRESS_LABEL_TYPE, chainType, tokenType) ++ hexBytes ++ Array[Byte](label),
      Array[Byte](LABEL_ADDRESS_TYPE, chainType, tokenType, label) ++ hexBytes
    )
  }

  def toTransactionKey(
      fromAddress: String,
      toAddress: String,
      txHash: String,
      chainType: Byte = CHAIN_TYPE,
      tokenType: Byte = TOKEN_TYPE
    ): (Array[Byte], Array[Byte]) = {
    val fromHexBytes = hexString2ArrayBytes(fromAddress)
    val toHexBytes = hexString2ArrayBytes(toAddress)
    val hexTxHash = hexString2ArrayBytes(txHash)

    (
      Array[Byte](OUT_TX_TYPE, chainType, tokenType) ++ fromHexBytes ++ toHexBytes ++ hexTxHash,
      Array[Byte](IN_TX_TYPE, chainType, tokenType) ++ toHexBytes ++ fromHexBytes ++ hexTxHash
    )
  }
}
