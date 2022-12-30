package com.tudb.blockchain.server

import java.io.{File, FileInputStream}
import java.util.Properties

/**
  *@description:
  */
object TuDBServerStarter {
  def main(args: Array[String]): Unit = {
//    val conf = args(0)
    // "./conf/tudb.conf"
    val context = getTuDBServerContext("./conf/tudb.conf")
    val server = new TuDBServer(context)
    server.start()
  }

  def getTuDBServerContext(conf: String): TuDBServerContext = {
    val properties = new Properties()
    properties.load(new FileInputStream(new File(conf)))

    val context = new TuDBServerContext()
    context.setTuDBPath(properties.getProperty("db-path"))
    context.setTuDBPort(properties.getProperty("tudb-port"))
    context.setEthNodeUrl(properties.getProperty("eth-node-url"))
    context
  }
}
