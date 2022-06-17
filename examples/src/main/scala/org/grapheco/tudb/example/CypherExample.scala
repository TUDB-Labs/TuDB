package org.grapheco.tudb.example

import org.apache.commons.io.FileUtils
import org.grapheco.lynx.types.structural.{LynxNode, LynxRelationship}
import org.grapheco.tudb.TuDBServer
import org.grapheco.tudb.client.TuClient
import org.grapheco.tudb.test.TestUtils

import java.io.File

/**
 * @author:John117
 * @createDate:2022/5/30
 * @description:
 */
object CypherExample {
  val port = 7600
  var server: TuDBServer = _
  var client: TuClient = _
  def main(args: Array[String]): Unit = {
    startServer()
    startClient()

    createNode()
    queryNode()
    createRelation()
    queryRelation()

    stopClient()
    shutdownServer()
  }

  def startServer(): Unit ={
    val dbPath: String = s"${TestUtils.getModuleRootPath}/testSpace/testBase"
    FileUtils.deleteDirectory(new File(dbPath))

    server = new TuDBServer(port, dbPath,f"db:///${TestUtils.getModuleRootPath}/testSpace/index")
    new Thread(new Runnable {
      override def run(): Unit = server.start()
    }).start()
  }
  def shutdownServer() = server.shutdown()

  def startClient(): Unit ={
    client = new TuClient("127.0.0.1", port)
  }
  def stopClient() = client.shutdown()

  def createNode(): Unit ={
    client.query("create (n:DataBase{name:'PandaDB'})")
    client.query("create (n: Company{name:'TUDB'})")
  }
  def queryNode(): Unit ={
    val res = client.query("match (n) return n")
    println()
    println("Query Node result: ")
    while (res.hasNext){
      val record = res.next()("n").asInstanceOf[LynxNode]
      showNode(record)
    }
    println()
  }

  def createRelation(): Unit ={
    client.query(
      """
        |match (n:DataBase{name:'PandaDB'})
        |match (c: Company{name:'TUDB'})
        |create (n)-[r: belongTo{year: 2022}]->(c)
        |""".stripMargin)
  }
  def queryRelation(): Unit ={
    val res = client.query("match (n)-[r: belongTo]->(m) return n,r,m")
    println()
    println("Query Relation Result: ")
    while (res.hasNext){
      val record = res.next()
      val leftNode = record("n").asInstanceOf[LynxNode]
      val relation = record("r").asInstanceOf[LynxRelationship]
      val rightNode = record("m").asInstanceOf[LynxNode]
      showNode(leftNode)
      showRelationship(relation)
      showNode(rightNode)
    }
  }
  def showNode(node: LynxNode): Unit ={
    println(s"Node<id: ${node.id.toLynxInteger.value}, labels: ${node.labels.map(l => l.value)}," +
      s" properties: ${node.keys.map(k => node.property(k).get.value)}>")
  }
  def showRelationship(r: LynxRelationship): Unit ={
    println(s"Relationship<id: ${r.id.toLynxInteger.value}, typeName: ${r.relationType.get.value}, " +
      s"properties: ${r.keys.map(k => r.property(k).get.value)}>")
  }
}
