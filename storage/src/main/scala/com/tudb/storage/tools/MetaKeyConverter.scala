package com.tudb.storage.tools

import com.tudb.blockchain.tools.ByteUtils

/**
  *@description:
  */
object MetaKeyConverter {
  val charset: String = "UTF-8"

  val chainType: Array[Byte] = "c".getBytes(charset)
  val tokenType: Array[Byte] = "t".getBytes(charset)
  val blockNumberType: Array[Byte] = "n".getBytes(charset)

  def getSynchronizedBlockNumberKey(blockchain: String): Array[Byte] = {
    blockNumberType ++ blockchain.getBytes(charset)
  }

  def getBlockchainKey(id: Int): Array[Byte] = {
    val intBytes = new Array[Byte](4)
    ByteUtils.setInt(intBytes, 0, id)
    chainType ++ intBytes
  }

  def getTokenKey(id: Int): Array[Byte] = {
    val intBytes = new Array[Byte](4)
    ByteUtils.setInt(intBytes, 0, id)
    tokenType ++ intBytes
  }
}
