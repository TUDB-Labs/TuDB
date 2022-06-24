package org.grapheco.tudb.FacadeTest

import org.apache.commons.io.FileUtils
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.{LynxInteger, LynxString}
import org.grapheco.lynx.types.structural.{LynxNode, LynxPropertyKey, LynxRelationship}
import org.grapheco.tudb.FacadeTest.GraphFacadeTest.db
import org.grapheco.tudb.test.TestUtils
import org.grapheco.tudb.{GraphDatabaseBuilder, TuInstanceContext}
import org.junit._
import org.junit.runners.MethodSorters

import java.io.File

/** @ClassName GraphFacadeTest
 * @Description TODO
 * @Author huchuan
 * @Date 2022/3/25
 * @Version 0.1
 */

object GraphFacadeTest {

  val outputPath: String = s"${TestUtils.getModuleRootPath}/facadeTest"
  val file = new File(outputPath)
  if (file.exists()) FileUtils.deleteDirectory(file)
  TuInstanceContext.setDataPath(outputPath)
  val db =
    GraphDatabaseBuilder.newEmbeddedDatabase(TuInstanceContext.getDataPath, "tudb://index?type=dummy")

  @AfterClass
  def onClose(): Unit = {
    db.close()
    if (file.exists()) FileUtils.deleteDirectory(file)
  }
}

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
class GraphFacadeTest {
  @After
  def clean(): Unit = {
    db.cypher("match (n) detach delete n")
  }

  @Test
  def testQueryNodeInNoDataDB(): Unit = {
    val res1 = db.cypher("match (n: Person) return n").records()
    val res2 = db.cypher("match (n) return n").records()
    val res3 = db.cypher("match (n)-[r]->(b) return r").records()
    val res4 = db.cypher("match (n)-[r: KNOWS]->(b) return r").records()
    Assert.assertEquals(0, res1.size)
    Assert.assertEquals(0, res2.size)
    Assert.assertEquals(0, res3.size)
    Assert.assertEquals(0, res4.size)
  }

  @Test
  def testQueryMultiLabelNode(): Unit = {
    db.cypher("create (n:Chengdu:Product1{name:'TUDB1'})")
    db.cypher("create (n:Chengdu:Product1{name:'TUDB2'})")
    db.cypher("create (n:Chengdu1:Product1{name:'TUDB3'})")

    val res =
      db.cypher("match (n:Chengdu1:Product1) return n").records().next()("n").asInstanceOf[LynxNode]
    Assert.assertEquals("TUDB3", res.property(LynxPropertyKey("name")).get.value)
  }

  //Test relationship's startId and endId
  @Test
  def testRelationship1(): Unit = {
    db.cypher("Create (n1:START)-[r1:rel]->(n2:Middle)-[r2:rel]->(n3:END)")
    val result = db
      .cypher(
        "Match p = (n1:START)-[r1:rel]->(n2:Middle)-[r2:rel]->(n3:END) return id(n1), r1, id(n2), r2, id(n3);"
      )
      .records()
      .next()
    val id1 = result.get("id(n1)").get.asInstanceOf[LynxInteger].value
    val id2 = result.get("id(n2)").get.asInstanceOf[LynxInteger].value
    val id3 = result.get("id(n3)").get.asInstanceOf[LynxInteger].value
    val r1 = result.get("r1").get.asInstanceOf[LynxRelationship].value
    val r2 = result.get("r2").get.asInstanceOf[LynxRelationship].value
    Assert.assertFalse(id1 == id2)
    Assert.assertFalse(id2 == id3)
    Assert.assertEquals(id1, r1.startNodeId.toLynxInteger.value)
    Assert.assertEquals(id2, r1.endNodeId.toLynxInteger.value)
    Assert.assertEquals(id2, r2.startNodeId.toLynxInteger.value)
    Assert.assertEquals(id3, r2.endNodeId.toLynxInteger.value)
  }

  // Set Relationship's Prop
  @Test
  def testRelationship2(): Unit = {
    db.cypher("Create (n1:START)-[r1:rel]->(n2:End)")
    val result1: LynxString = db
      .cypher(
        "Match p = (n1:START)-[r1:rel]->(n2:End) Set r1.prop1='prop1' return r1"
      )
      .records()
      .next()
      .get("r1")
      .get
      .asInstanceOf[LynxRelationship]
      .property(LynxPropertyKey("prop1"))
      .get
      .asInstanceOf[LynxString]
    Assert.assertEquals("prop1", result1.value)

    val result2 = db
      .cypher("Match p = (n1:START)-[r1:rel]->(n2:End) return r1")
      .records()
      .next()
      .get("r1")
      .get
      .asInstanceOf[LynxRelationship]
      .property(LynxPropertyKey("prop1"))
      .get
      .asInstanceOf[LynxString]
    Assert.assertEquals("prop1", result2.value)
  }

  @Test
  def testRelationship3(): Unit = {
    db.cypher("Create (n1:START)-[r1:rel{prop1:'prop1'}]->(n2:End)")
    val result1 = db
      .cypher("Match p = (n1:START)-[r1:rel]->(n2:End) return r1")
      .records()
      .next()
      .get("r1")
      .get
      .asInstanceOf[LynxRelationship]
      .property(LynxPropertyKey("prop1"))
      .get
      .asInstanceOf[LynxString]
    Assert.assertEquals("prop1", result1.value)
    val result2 = db
      .cypher(
        "Match p = (n1:START)-[r1:rel]->(n2:End) remove r1.prop1 return r1;"
      )
      .records()
      .next()
      .get("r1")
      .get
      .asInstanceOf[LynxRelationship]
      .property(LynxPropertyKey("prop1"))
    result2 match {
      case None => Assert.assertTrue(true)
      case _ => Assert.assertTrue(false)
    }
    val result3 = db
      .cypher("Match p = (n1:START)-[r1:rel]->(n2:End) return r1")
      .records()
      .next()
      .get("r1")
      .get
      .asInstanceOf[LynxRelationship]
      .property(LynxPropertyKey("prop1"))
    result3 match {
      case None => Assert.assertTrue(true)
      case _ => Assert.assertTrue(false)
    }

  }

  // Remove node's prop.
  @Test
  def testRemoveNodeProp(): Unit = {
    db.cypher("Create(n:TestRemoveProp{prop1:'prop1', prop2:'prop2'})")
    val prop1 = db
      .cypher("Match(n:TestRemoveProp) Return n;")
      .records()
      .next()
      .get("n")
      .get
      .asInstanceOf[LynxNode]
      .property(LynxPropertyKey("prop1"))
      .get
      .asInstanceOf[LynxString]
      .value
    Assert.assertEquals("prop1", prop1)
    val deletedProp1: Option[LynxValue] = db
      .cypher("Match(n:TestRemoveProp) remove n.prop1 Return n;")
      .records()
      .next()
      .get("n")
      .get
      .asInstanceOf[LynxNode]
      .property(LynxPropertyKey("prop1"))
    deletedProp1 match {
      case None => Assert.assertTrue(true)
      case _ => Assert.assertTrue(false)
    }
  }
}
