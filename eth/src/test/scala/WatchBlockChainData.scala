import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.tudb.blockchain.eth.{EthJsonParser, EthNodeClient, EthNodeJsonApi}

import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.JavaConverters._

/**
  *@description:
  */
object WatchBlockChainData {
  def main(args: Array[String]): Unit = {
    val queue = new ConcurrentLinkedQueue[JSONObject]()
    val client = new EthNodeClient("192.168.31.178", 8546, queue)
    client.connect

    client.sendJsonRequest(EthNodeJsonApi.getEthBlockNumber(1))
    val currentBlockNumber = EthJsonParser.getBlockNumber(client.consumeResult())

    client.sendJsonRequest(EthNodeJsonApi.getBlockByNumber(currentBlockNumber, true, 1))

    val jsonResult = client.consumeResult()
    val transactions = jsonResult.get("result")
    client.close()
  }
}
