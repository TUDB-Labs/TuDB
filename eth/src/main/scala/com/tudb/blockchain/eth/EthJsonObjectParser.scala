package com.tudb.blockchain.eth

import com.alibaba.fastjson.{JSONArray, JSONObject}

/**
  *@description:
  */
object EthJsonObjectParser {
  def getBlockNumber(block: JSONObject): Int = {
    val value = block.get("result").toString
    Integer.parseInt(value.drop(2), 16)
  }

  def getBlockTransaction(block: JSONObject): Array[EthTransaction] = {
    try {
      val blockInfo = block.get("result").asInstanceOf[JSONObject]
      val timeStamp = blockInfo.get("timestamp").toString
      val transactions = blockInfo
        .get("transactions")
        .asInstanceOf[JSONArray]
        .toArray
        .map(any => any.asInstanceOf[JSONObject]) // transactions
        .map(jsonObj =>
          EthTransaction(
            jsonObj.getString("from"),
            jsonObj.getString("to"),
            timeStamp,
            jsonObj.getString("hash"),
            jsonObj.getString("value")
          )
        )
        .filter(tx => tx.from != null && tx.to != null)

      transactions
    } catch {
      case e: Exception => {
        Array.empty
      }
    }
  }
}

case class EthTransaction(from: String, to: String, timeStamp: String, txHash: String, wei: String)
