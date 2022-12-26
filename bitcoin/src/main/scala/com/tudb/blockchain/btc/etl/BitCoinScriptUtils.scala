package com.tudb.blockchain.btc.etl

import org.apache.commons.codec.binary.Hex
import org.bitcoinj.core.{Base58, Utils, Sha256Hash}

import java.security.MessageDigest

/**
  *@description:
  */
object BitCoinScriptUtils {
  def scriptHexToNonStandardAddress(scriptHex: String): String = {
    Option(scriptHex)
      .map(hex => {
        "nonstandard" + Hex
          .encodeHexString(
            MessageDigest
              .getInstance("SHA-256")
              .digest(Hex.decodeHex(hex))
          )
          .substring(0, 40)
      })
      .orNull
  }

  def pubkeyToAddress(scriptAsm: String): String = {
    Option(scriptAsm)
      .map(asm => {
        val id = Utils.sha256hash160(Hex.decodeHex(scriptAsm.split(" ")(0)))
        val idHash = Sha256Hash.hashTwice(Array[Byte](0) ++ id)
        val base58 = Base58.encode(id ++ idHash.slice(0, 4))
        s"1${base58}"
      })
      .orNull
  }
}
