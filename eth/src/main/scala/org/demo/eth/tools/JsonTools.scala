package org.demo.eth.tools

import com.alibaba.fastjson.{JSONArray, JSONObject}

/**
  *@description:
  */
object JsonTools {

  def getBlockNumber(block: JSONObject): Int = {
    val value = block.get("result").toString
    Integer.parseInt(value.slice(2, value.length), 16)
  }

  def getBlockTransaction(block: JSONObject): Array[EthTransaction] = {
    block
      .get("result")
      .asInstanceOf[JSONObject]
      .get("transactions")
      .asInstanceOf[JSONArray]
      .toArray
      .map(any => any.asInstanceOf[JSONObject]) // transactions
      .map(jsonObj =>
        EthTransaction(
          jsonObj.getString("from"),
          jsonObj.getString("to"),
          jsonObj.getString("hash"),
          jsonObj.getString("value")
        )
      )
      .filter(tx => tx.from != null && tx.to != null && tx.wei != "0x0" && tx.wei != null)
  }
}

case class EthTransaction(from: String, to: String, txHash: String, wei: String)
