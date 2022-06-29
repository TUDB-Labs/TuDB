/** Copyright (c) 2022 PandaDB * */
package org.grapheco.tudb.example

import org.grapheco.lynx.types.structural.LynxNode
import org.grapheco.tudb.client.TuDBClient
import org.grapheco.tudb.example.CypherExample.{showNode, startServer}

import java.io.{FileInputStream, FileOutputStream}
import java.util.zip.ZipInputStream
import java.io.File
import java.net.URL
import sys.process._

/** @author:huanglin
  * @createDate:2022/5/30
  * @description: this is benchmark for TuDB index engine
  */
object BenchmarkExampleForIndex {
  val port = 7600
  def main(args: Array[String]): Unit = {
    //download the data
    val path = downloadData("https://cdcdn.tudb.work/ldbc/0.003.zip")
    //get index url
    val indexUrl = if (args.length > 0) args(0) else "tudb://index?type=dummy"
    //start server
    val indexServer = startServer(path, port, indexUrl)

    val indexClient = new TuDBClient("127.0.0.1", port)

    val indexUseTime1 = calMethod(indexClient, queryPropertyNode)
    val indexUseTime2 = calMethod(indexClient, queryPropertyNode)
    val indexUseTime3 = calMethod(indexClient, queryPropertyNode)
    val indexUseTime4 = calMethod(indexClient, queryPropertyNode)
    val indexUseTime5 = calMethod(indexClient, queryPropertyNode)

    println("index use time:" + indexUseTime1)
    println("index use time:" + indexUseTime2)
    println("index use time:" + indexUseTime3)
    println("index use time:" + indexUseTime4)
    println("index use time:" + indexUseTime5)

    indexClient.shutdown()
    indexServer.shutdown()

  }

  def calMethod(client: TuDBClient, fun: TuDBClient => Long): Long = {
    val useTime = fun(client)
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
}
