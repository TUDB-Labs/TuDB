import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.tudb.blockchain.eth.client.{EthJsonObjectParser, EthNodeClient, EthNodeJsonApi}

import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.JavaConverters._

/**
  *@description:
  */
object WatchBlockChainData {
  def main(args: Array[String]): Unit = {
    val queue = new ConcurrentLinkedQueue[JSONObject]()
    val client = new EthNodeClient("192.168.31.40", 8546)
    client.connect

    client.sendJsonRequest(EthNodeJsonApi.getEthBlockNumber(1))
    val currentBlockNumber = EthJsonObjectParser.getBlockNumber(client.consumeMessage())

    client.sendJsonRequest(EthNodeJsonApi.getBlockByNumber(currentBlockNumber, true, 1))

    val jsonResult = client.consumeMessage()
    val transactions = jsonResult.get("result")
    println(transactions)
    client.close()
  }
}
