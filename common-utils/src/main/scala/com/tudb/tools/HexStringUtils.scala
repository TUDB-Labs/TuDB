package com.tudb.tools

import scala.collection.mutable

/**
  *@description:
  */
object HexStringUtils {
  val OUT_TX_TYPE: Byte = 'o'.toByte
  val IN_TX_TYPE: Byte = 'i'.toByte

  def removeHexStringHeader(str: String): String = {
    if (str.startsWith("0x")) str.drop(2)
    else str
  }

  def arrayBytes2HexString(arrayByte: Array[Byte]): String = {
    if (arrayByte.sameElements(Array(0))) return "0"

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
    if (hexString == "0") return Array(0)

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
}
