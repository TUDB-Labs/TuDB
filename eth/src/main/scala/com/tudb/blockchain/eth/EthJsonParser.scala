package com.tudb.blockchain.eth

import com.alibaba.fastjson.{JSONArray, JSONObject}

/**
  *@description:
  */
object EthJsonParser {
  def getBlockNumber(block: JSONObject): Int = {
    val value = block.get("result").toString
    Integer.parseInt(value.drop(2), 16)
  }

  def getBlockTransaction(block: JSONObject): Array[EthTransaction] = {
    try {
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
    } catch {
      case e: Exception => {
        Array.empty
      }
    }
  }
}

case class EthTransaction(from: String, to: String, txHash: String, wei: String)
