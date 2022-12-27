package com.tudb.storage.tools

/**
  *@description:
  */
object MetaKeyConverter {
  val charset: String = "UTF-8"
  val chainType: Array[Byte] = "c".getBytes(charset)
  val tokenType: Array[Byte] = "t".getBytes(charset)
  val labelType: Array[Byte] = "l".getBytes(charset)
  val blockNumberType: Array[Byte] = "n".getBytes(charset)

  def getSynchronizedBlockNumberKey(blockchain: String): Array[Byte] = {
    blockNumberType ++ blockchain.getBytes(charset)
  }

  def getBlockchainKey(blockchain: String): Array[Byte] = {
    chainType ++ blockchain.getBytes(charset)
  }

  def getTokenKey(token: String): Array[Byte] = {
    tokenType ++ token.getBytes(charset)
  }

  def getLabelKey(label: String): Array[Byte] = {
    labelType ++ label.getBytes(charset)
  }

  def metaKeyToString(key: Array[Byte]): String = {
    new String(key.drop(1), charset)
  }
}
