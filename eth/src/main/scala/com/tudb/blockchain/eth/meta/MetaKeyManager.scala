package com.tudb.blockchain.eth.meta

/**
  *@description:
  */
object MetaKeyManager {
  // store currently synchronized block number
  val blockNumberKey: Array[Byte] = Array[Byte]('b'.toByte, 1.toByte)

}
