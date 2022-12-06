package com.tudb.blockchain.eth.client

import com.alibaba.fastjson.{JSON, JSONObject}
import org.java_websocket.client.WebSocketClient
import org.java_websocket.handshake.ServerHandshake

import java.net.URI
import java.util.concurrent.ConcurrentLinkedQueue

/**
  *@description:
  */
class EthNodeClient(host: String, port: Int) {
  private val msgQueue: ConcurrentLinkedQueue[JSONObject] = new ConcurrentLinkedQueue[JSONObject]()

  private val client = new WebSocketClient(new URI(s"ws://$host:$port")) {
    override def onOpen(serverHandshake: ServerHandshake): Unit = {}

    override def onMessage(msg: String): Unit = {
      val obj = JSON.parseObject(msg)
      msgQueue.offer(obj)
    }

    override def onClose(i: Int, s: String, b: Boolean): Unit = {}

    override def onError(e: Exception): Unit = {
      println(s"error: $e")
    }
  }

  def connect: Unit = {
    client.connect()
    var count = 0
    while (!client.isOpen) {
      println("Connecting to Eth Node.....")
      count += 1
      Thread.sleep(1000)
      if (count > 3) {
        println("Connection refused....")
        System.exit(1)
      }
    }
    println("Connected to Eth Node")
  }

  def isConnected: Boolean = client.isOpen

  def reconnect: Unit = client.reconnect()

  def close(): Unit = client.close()

  def sendJsonRequest(jsonRequest: String): Unit = {
    client.send(jsonRequest)
  }

  def consumeMessage(): JSONObject = msgQueue.poll()

  def isMessageEmpty(): Boolean = msgQueue.isEmpty
}
