package com.tudb.blockchain.btc.etl

import org.json4s.{DefaultFormats, Formats, JNull, JValue}
import org.json4s.native.JsonMethods
import scalaj.http.{Http, HttpResponse}

/**
  *@description:
  */
class BtcRequests {
  implicit val formats: Formats = DefaultFormats

  private def requestBlockHash(height: Int): JValue = {
    val response = Http("http://192.168.31.186:8332")
      .auth("bitcoinrpc", "snSlZH0XfVhqotiD8BH4mOgUH2lt1AzH3gnWmKbJFMF3")
      .header("Content-type", "application/json")
      .method("post")
      .postData(
        BtcJsonRpcString.getBlockHash(height)
      )
      .asString
    if (response.code == 200) JsonMethods.parse(response.body) \ "result" else JNull
  }

  def requestBlock(height: Int): JValue = {
    var retry = 0
    while (retry < 3) {
      val txHash = requestBlockHash(height).extractOpt[String].getOrElse("")
      val response = Http("http://192.168.31.186:8332")
        .auth("bitcoinrpc", "snSlZH0XfVhqotiD8BH4mOgUH2lt1AzH3gnWmKbJFMF3")
        .header("Content-type", "application/json")
        .method("post")
        .postData(
          BtcJsonRpcString.getBlock(txHash, 2)
        )
        .asString
      if (response.code == 200) {
        return JsonMethods.parse(response.body) \ "result"
      } else {
        retry += 1
        Thread.sleep(1000)
      }
    }
    println(s"Cannot get block height: ${height}")
    JNull
  }

  def requestTx(txId: String): JValue = {
    var retry = 0
    while (retry < 3) {
      val response: HttpResponse[String] = Http(s"http://192.168.31.186:8332")
        .auth("bitcoinrpc", "snSlZH0XfVhqotiD8BH4mOgUH2lt1AzH3gnWmKbJFMF3")
        .header("Content-type", "application/json")
        .method("post")
        .postData(
          BtcJsonRpcString.getTransaction(txId, true)
        )
        .asString
      if (response.code == 200) {
        return JsonMethods.parse(response.body) \ "result"
      } else {
        retry += 1
        Thread.sleep(1000)
      }
    }
    println(s"Cannot get transaction: ${txId}")
    JNull
  }
}
