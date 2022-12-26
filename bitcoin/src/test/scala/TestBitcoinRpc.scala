import com.tudb.blockchain.btc.etl.BtcJsonRpcString
import org.json4s.JsonAST.JArray
import org.json4s.{DefaultFormats, Formats, JNull, JValue}
import org.json4s.native.JsonMethods
import scalaj.http.{Http, HttpResponse}

/**
  *@description:
  */
object TestBitcoinRpc {
  implicit val formats: Formats = DefaultFormats
  def main(args: Array[String]): Unit = {
    val result2 = requestBlockHash(760000).extractOpt[String]

    result2.foreach(blockHash => {
      println(blockHash)
      val res = requestBlock(blockHash, 2)
      val parsed = JsonMethods.parse(res.body) \ "result"

      println("")
    })

  }

  def requestBlockHash(blockNumber: Int): JValue = {
    val response = Http("http://192.168.31.186:8332")
      .auth("bitcoinrpc", "snSlZH0XfVhqotiD8BH4mOgUH2lt1AzH3gnWmKbJFMF3")
      .header("Content-type", "application/json")
      .method("post")
      .postData(
        BtcJsonRpcString.getBlockHash(blockNumber)
      )
      .asString
    if (response.code == 200) JsonMethods.parse(response.body) \ "result" else JNull
  }

  def requestBlock(blockHash: String, verbosity: Int): HttpResponse[String] = {
    val request = Http("http://192.168.31.186:8332")
      .auth("bitcoinrpc", "snSlZH0XfVhqotiD8BH4mOgUH2lt1AzH3gnWmKbJFMF3")
      .header("Content-type", "application/json")
      .method("post")
      .postData(
        BtcJsonRpcString.getBlock(blockHash, verbosity)
      )
      .asString

    request
  }

  def requestTransaction(txid: String, verbosity: Boolean): HttpResponse[String] = {
    val request = Http("http://192.168.31.186:8332")
      .auth("bitcoinrpc", "snSlZH0XfVhqotiD8BH4mOgUH2lt1AzH3gnWmKbJFMF3")
      .header("Content-type", "application/json")
      .method("post")
      .postData(
        BtcJsonRpcString.getTransaction(txid, verbosity)
      )
      .asString
    request
  }

  def requestBlockCount(): JValue = {
    val response = Http("http://192.168.31.186:8332")
      .auth("bitcoinrpc", "snSlZH0XfVhqotiD8BH4mOgUH2lt1AzH3gnWmKbJFMF3")
      .header("Content-type", "application/json")
      .method("post")
      .postData(
        s"""{"jsonrpc": "2.0", "id": 0, "method": "getblockcount", "params": []}"""
      )
      .asString

    if (response.code == 200) JsonMethods.parse(response.body) \ "result" else JNull
  }
}

// 768018
