package com.tudb.blockchain.btc.etl

import org.json4s.JsonAST.JArray
import org.json4s.{DefaultFormats, Formats, JNull, JValue}
import org.json4s.native.JsonMethods.parse
import scalaj.http.{Http, HttpResponse}

/**
  *@description:
  */
object BtcTransactionEtl {
  def main(args: Array[String]): Unit = {
    val etl = new BtcTransactionEtl
    etl.process(760000)
  }
}

class BtcTransactionEtl {
  implicit val formats: Formats = DefaultFormats
  val btcRequests = new BtcRequests()

  def process(height: Int): Unit = {
    val jsonBlock = btcRequests.requestBlock(height)
    val timestamp = (jsonBlock \ "time").extract[Long]
    val jsonTxs = (jsonBlock \ "tx").asInstanceOf[JArray].arr
    jsonTxs.foreach(tx => {
      processOneTx(height, timestamp, tx)
    })
    println("")
  }

  def processOneTx(height: Int, timestamp: Long, tx: JValue): Unit = {
    val inputs = (tx \ "vin")
      .asInstanceOf[JArray]
      .arr
      .map(in => {
        if ((in \ "coinbase").toOption.isEmpty) {
          val prevTxId = (in \ "txid").extract[String]
          val vout = (in \ "vout").extract[Int]

          val prevTx = btcRequests.requestTx(prevTxId)
          val prevOut = (prevTx \ "vout").asInstanceOf[JArray].arr(vout)
          val script = prevOut \ "scriptPubKey"

          val amount = ((prevOut \ "value").extract[Double] * math.pow(10, 8)).asInstanceOf[Long]
          val address = (script \ "address").toOption.map(_.extract[String]).orNull
          val addressType = (script \ "type").extract[String]
          val scriptAsm = (script \ "asm").extract[String]
          val scriptHex = (script \ "hex").extract[String]

          address match {
            case null => {
              addressType match {
                case "pubkey" => (BitCoinScriptUtils.pubkeyToAddress(scriptAsm), amount)
                case _        => (BitCoinScriptUtils.scriptHexToNonStandardAddress(scriptHex), amount)
              }
            }
            case _ => (address, amount)
          }
        } else {
          ("coinbase", 0)
        }
      })

    val outputs = (tx \ "vout")
      .asInstanceOf[JArray]
      .arr
      .map(out => {
        val script = out \ "scriptPubKey"
        val amount = ((out \ "value").extract[Double] * math.pow(10, 8)).asInstanceOf[Long]

        val address = (script \ "address").toOption.map(_.extract[String]).orNull
        val addressType = (script \ "type").extract[String]
        val scriptAsm = (script \ "asm").extract[String]
        val scriptHex = (script \ "hex").extract[String]

        address match {
          case null => {
            addressType match {
              case "pubkey" => (BitCoinScriptUtils.pubkeyToAddress(scriptAsm), amount)
              case _        => (BitCoinScriptUtils.scriptHexToNonStandardAddress(scriptHex), amount)
            }
          }
          case _ => (address, amount)
        }
      })

    println("")
  }
}

case class Transaction(fromAddresses: Seq[String], toAddresses: Seq[(String, Long)])
