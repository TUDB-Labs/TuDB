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

  def setEthNodeHost(host: String): Unit = {
    map.put("eth-node-host", host)
  }
  def getEthNodeHost(): String = {
    val res = map.get("eth-node-host")
    if (res.isDefined) res.get
    else throw new Exception("cannot read eth node host.")
  }

  def setTuDBPort(port: String): Unit = {
    map.put("tudb-port", port)
  }

  def getTuDBPort(): Int = {
    val res = map.get("tudb-port")
    if (res.isDefined) Integer.parseInt(res.get)
    else throw new Exception("cannot read tudb port.")
  }

  def setEthNodePort(port: String): Unit = {
    map.put("eth-node-port", port)
  }

  def getEthNodePort(): Int = {
    val res = map.get("eth-node-port")
    if (res.isDefined) Integer.parseInt(res.get)
    else throw new Exception("cannot read eth node port.")
  }

  def setEthNodeSynchronizeSpeed(ms: String): Unit = {
    map.put("eth-node-synchronize-speed", ms)
  }

  def getEthNodeSynchronizeSpeed(): Int = {
    val res = map.get("eth-node-synchronize-speed")
    if (res.isDefined) Integer.parseInt(res.get, 10)
    else throw new Exception("cannot read eth synchronize speed.")
  }
}
