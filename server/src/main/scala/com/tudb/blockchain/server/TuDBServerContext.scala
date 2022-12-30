package com.tudb.blockchain.server

import scala.collection.mutable

/**
  *@description:
  */
class TuDBServerContext {
  private val map = mutable.Map[String, String]();

  def setTuDBPath(dbPath: String): Unit = {
    map.put("db-path", dbPath)
  }
  def getTuDBPath(): String = {
    val res = map.get("db-path")
    if (res.isDefined) res.get
    else throw new Exception("cannot read tudb path.")
  }

  def setTuDBPort(port: String): Unit = {
    map.put("tudb-port", port)
  }

  def getTuDBPort(): Int = {
    val res = map.get("tudb-port")
    if (res.isDefined) Integer.parseInt(res.get)
    else throw new Exception("cannot read tudb port.")
  }

  def setEthNodeUrl(url: String): Unit = {
    map.put("eth-node-url", url)
  }
  def getEthNodeUrl(): String = {
    val res = map.get("eth-node-url")
    if (res.isDefined) res.get
    else throw new Exception("cannot read eth node url.")
  }
}
