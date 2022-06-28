package org.grapheco.tudb.example

import org.apache.commons.io.FileUtils
import org.grapheco.lynx.types.structural.{LynxNode, LynxRelationship}
import org.grapheco.tudb.client.TuDBClient
import org.grapheco.tudb.test.TestUtils
import org.grapheco.tudb.{TuDBServer, TuDBServerContext}

import java.io.{FileInputStream, FileOutputStream}
import java.util.zip.ZipInputStream
import java.io.File
import java.net.URL
import sys.process._

/** @author:John117
  * @createDate:2022/5/30
  * @description:
  */
object BenchmarkExample {
  val port = 7600
//  var server: TuDBServer = _
//  var client: TuDBClient = _
  def main(args: Array[String]): Unit = {
    val path = downloadData("https://cdcdn.tudb.work/ldbc/0.003.zip")
    val useIndex = if (args.length > 0) args(0).toBoolean else false
    val indexServer = startServer(path, port, "tudb://index?type=dummy")

    val indexClient = startClient(port)

    val indexUseTime1 = calMethod(indexClient, queryPropertyNode)
    val indexUseTime2 = calMethod(indexClient, queryPropertyNode)
    val indexUseTime3 = calMethod(indexClient, queryPropertyNode)
    val indexUseTime4 = calMethod(indexClient, queryPropertyNode)

    println("index use time:" + indexUseTime1)
    println("index use time:" + indexUseTime2)
    println("index use time:" + indexUseTime3)
    println("index use time:" + indexUseTime4)

    stopClient(indexClient)
    shutdownServer(indexServer)
//    Thread.sleep(6000)
//
//    val server = startServer(path, port )
//    val client = startClient(port)
//    val useTime = calMethod(client, queryPropertyNode)
//    println("use time:" + useTime)
//
//    stopClient(client)
//    shutdownServer(server)

  }

  def calMethod(client: TuDBClient, fun: TuDBClient => Long): Long = {
    val useTime = fun(client)
//    println("use time:" + (useTime))
    useTime
  }

  /** download data from url,and extract it to path
    * @param url
    * @param fileName
    * @return path
    */
  def downloadData(url: String, fileName: String = "data.zip"): String = {
    //save network data to local file
    if (!new File(fileName).exists()) {
      new URL(url) #> new File(fileName) !!
    }
    //extract data to local directory
    val zis = new ZipInputStream(new FileInputStream(fileName))
    var dictionaryName = ""
    Stream.continually(zis.getNextEntry).takeWhile(_ != null).foreach { file =>
      if (file.isDirectory) {
        new File(file.getName).mkdirs
        dictionaryName = file.getName
      } else {
        val fout = new FileOutputStream(file.getName)
        val buffer = new Array[Byte](1024)
        Stream.continually(zis.read(buffer)).takeWhile(_ != -1).foreach(fout.write(buffer, 0, _))
      }
    }
    //return dictionary path
    dictionaryName.split("/").headOption.getOrElse("")
  }

  def startServer(
      dbPath: String,
      port: Int,
      indexUri: String = "tudb://index?type=dummy"
    ): TuDBServer = {
    val serverContext = new TuDBServerContext()
    serverContext.setPort(port)
    serverContext.setDataPath(dbPath)
    serverContext.setIndexUri(indexUri)
    val server = new TuDBServer(serverContext)
    new Thread(new Runnable {
      override def run(): Unit = server.start()
    }).start()
    server
  }
  def shutdownServer(server: TuDBServer) = server.shutdown()

  def startClient(port: Int): TuDBClient = {
    new TuDBClient("127.0.0.1", port)
  }
  def stopClient(client: TuDBClient) = client.shutdown()

  def queryNode(client: TuDBClient): Unit = {
    val res = client.query("match (n) return n limit 10")
    println()
    println("Query Node result: ")
    while (res.hasNext) {
      val record = res.next()("n").asInstanceOf[LynxNode]
      showNode(record)
    }
    println()
  }
  def queryPropertyNode(client: TuDBClient): Long = {
    val time = System.currentTimeMillis()
    val res = client.query("match (n:Tag) where n.name='Rumi' return n limit 10")
    println()
    println("Query Node result: ")
    while (res.hasNext) {
      val record = res.next()("n").asInstanceOf[LynxNode]
      showNode(record)
    }
    println()
    System.currentTimeMillis() - time
  }

  def showNode(node: LynxNode): Unit = {
    println(
      s"Node<id: ${node.id.toLynxInteger.value}, labels: ${node.labels.map(l => l.value)}," +
        s" properties: ${node.keys.map(k => k.value + "->" + node.property(k).get.value)}>"
    )
  }
}
