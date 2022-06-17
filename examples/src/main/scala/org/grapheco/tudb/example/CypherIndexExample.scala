package main.scala.org.grapheco.tudb.example

import org.grapheco.lynx.types.structural.{LynxNode, LynxRelationship}
import org.grapheco.tudb.{GraphDatabaseBuilder}
import org.grapheco.tudb.facade.GraphFacade
import org.grapheco.tudb.test.TestUtils


/**
 * @author:John117
 * @createDate:2022/5/30
 * @description:
 */
object CypherIndexExample {
  var db:GraphFacade=_
  def main(args: Array[String]): Unit = {
    startServer()

    queryNode()
    queryNode()
    queryNode()

    queryRelation()

    shutdownServer()
  }

  def startServer(): Unit ={
    val dbPath: String = s"${TestUtils.getModuleRootPath}/testSpace/ldbc0.003.db"
    db = GraphDatabaseBuilder.newEmbeddedDatabase(dbPath,f"none")
  }
  def shutdownServer() = db.close()



  def queryNode(): Unit ={
    val res = db.cypher("match (n:Person) where n.firstName='Ali' return n limit 10")
    println()
    val ts=System.currentTimeMillis()
    println("Query Node result: ")
    val rs=res.records()
    while (rs.hasNext){
      val record = rs.next()("n").asInstanceOf[LynxNode]
      showNode(record)
    }
    println(System.currentTimeMillis()-ts)
  }

  def queryRelation(): Unit ={
    val res = db.cypher("match (n: Person)-[r: KNOWS]->(m: Person) return n,r,m limit 10")
    println()
    println("Query Relation Result: ")
    val rs=res.records()
    while (rs.hasNext){
      val record = rs.next()
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
