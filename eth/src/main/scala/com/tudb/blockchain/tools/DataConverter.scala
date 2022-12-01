package com.tudb.blockchain.tools

import com.tudb.blockchain.eth.EthKeyConverter.{ADDRESS_LABEL_TYPE, IN_TX_TYPE, LABEL_ADDRESS_TYPE, OUT_TX_TYPE}

import scala.collection.mutable

/**
  *@description:
  */
object DataConverter {
  def removeHexStringHeader(str: String): String = {
    if (str.startsWith("0x")) str.drop(2)
    else str
  }
  def arrayBytes2HexString(arrayByte: Array[Byte]): String = {
    val hexStringBuilder = new mutable.StringBuilder(arrayByte.length * 2, "")
    for (byte <- arrayByte) {
      val hex = Integer.toHexString(byte & 0xFF)
      if (hex.length == 1) hexStringBuilder.append("0")
      hexStringBuilder.append(hex)
    }
    hexStringBuilder.toString()
  }

  // hexString can not start with '0x'
  def hexString2ArrayBytes(hexString: String): Array[Byte] = {
    val hexStr = {
      if (hexString.length % 2 == 1) "0" + hexString
      else hexString
    }
    val charArray = hexStr.toCharArray
    val byteLength = hexStr.length / 2
    val bytesArray = new Array[Byte](byteLength)

    // one hex string = 4 bit, two hex string = 1 byte
    for (i <- 0 until byteLength) {
      val k = i * 2
      val high = Character.digit(charArray(k), 16) & 0xFF
      val low = Character.digit(charArray(k + 1), 16) & 0xFF
      bytesArray(i) = (high << 4 | low).toByte
    }

    bytesArray
  }

  def decode2Address(key: Array[Byte]): String = {
    key.head match {
      case ADDRESS_LABEL_TYPE => arrayBytes2HexString(key.slice(3, 23))
      case LABEL_ADDRESS_TYPE => arrayBytes2HexString(key.drop(4))
    }
  }
  def decode2Transaction(key: Array[Byte]): (String, String, String) = {
    key.head match {
      case OUT_TX_TYPE => {
        val from = arrayBytes2HexString(key.slice(3, 23))
        val to = arrayBytes2HexString(key.slice(23, 43))
        val txHash = arrayBytes2HexString(key.slice(43, key.length))
        (from, to, txHash)
      }
      case IN_TX_TYPE => {
        val to = arrayBytes2HexString(key.slice(3, 23))
        val from = arrayBytes2HexString(key.slice(23, 43))
        val txHash = arrayBytes2HexString(key.slice(43, key.length))
        (from, to, txHash)
      }
    }
  }
}
