package org.grapheco.tudb.FacadeTest

import org.apache.commons.io.FileUtils
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.{LynxInteger, LynxString}
import org.grapheco.lynx.types.structural.{LynxNode, LynxPropertyKey, LynxRelationship}
import org.grapheco.lynx.types.time.LynxDateTime
import org.grapheco.lynx.util.LynxDateTimeUtil
import org.grapheco.tudb.FacadeTest.GraphFacadeTest.db
import org.grapheco.tudb.test.TestUtils
import org.grapheco.tudb.{GraphDatabaseBuilder, TuDBInstanceContext}
import org.junit._
import org.junit.runners.MethodSorters

import java.io.File
import java.time.ZonedDateTime
import java.util.regex.Pattern

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
  TuDBInstanceContext.setDataPath(outputPath)
  val db =
    GraphDatabaseBuilder.newEmbeddedDatabase(
      TuDBInstanceContext.getDataPath,
      "tudb://index?type=dummy"
    )

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

  private def initHaveCircleGraphData(): Unit = {
    db.cypher("create (n:person{age:1})")
    db.cypher("create (n:person{age:2})")
    db.cypher("create (n:person{age:3})")

    db.cypher("""
                |match (n:person{age: 1})
                |match (m: person{age:2})
                |create (n)-[r:KNOW]->(m)
                |""".stripMargin)
    db.cypher("""
                |match (n:person{age: 2})
                |match (m: person{age:3})
                |create (n)-[r:KNOW]->(m)
                |""".stripMargin)
    db.cypher("""
                |match (n:person{age: 3})
                |match (m: person{age:1})
                |create (n)-[r:KNOW]->(m)
                |""".stripMargin)
  }

  private def initManualExample(): Unit = {
    db.cypher("create (n: Person{name:'Oliver Stone'})")
    db.cypher("create (n: Person{name:'Michael Douglas'})")
    db.cypher("create (n: Person{name:'Charlie Sheen'})")
    db.cypher("create (n: Person{name:'Martin Sheen'})")
    db.cypher("create (n: Person{name:'Rob Reiner'})")
    db.cypher("create (n: Movie{title:'Wall Street'})")
    db.cypher("create (n: Movie{title:'The American President'})")

    db.cypher("""
                |match (n: Person{name:'Oliver Stone'})
                |match (m: Movie{title:'Wall Street'})
                |create (n)-[r: DIRECTED]->(m)
                |""".stripMargin)
    db.cypher("""
                |match (n: Person{name:'Michael Douglas'})
                |match (m: Movie{title:'Wall Street'})
                |create (n)-[r: ACTED_IN{role: 'Gordon Gekko'}]->(m)
                |""".stripMargin)
    db.cypher("""
                |match (n: Person{name:'Charlie Sheen'})
                |match (m: Movie{title:'Wall Street'})
                |create (n)-[r: ACTED_IN{role: 'Bud Fox'}]->(m)
                |""".stripMargin)
    db.cypher("""
                |match (n: Person{name:'Martin Sheen'})
                |match (m: Movie{title:'Wall Street'})
                |create (n)-[r: ACTED_IN{role: 'Carl Fox'}]->(m)
                |""".stripMargin)
    db.cypher("""
                |match (n: Person{name:'Michael Douglas'})
                |match (m: Movie{title:'The American President'})
                |create (n)-[r: ACTED_IN{role: 'President Andrew Shepherd'}]->(m)
                |""".stripMargin)
    db.cypher("""
                |match (n: Person{name:'Martin Sheen'})
                |match (m: Movie{title:'The American President'})
                |create (n)-[r: ACTED_IN{role: 'A.J. MacInerney'}]->(m)
                |""".stripMargin)
    db.cypher("""
                |match (n: Person{name:'Rob Reiner'})
                |match (m: Movie{title:'The American President'})
                |create (n)-[r: DIRECTED]->(m)
                |""".stripMargin)
  }
  private def initOutGoingExample(): Unit = {
    db.cypher("create (n:person{nid: 1})")
    db.cypher("create (n:person{nid: 2})")
    db.cypher("create (n:person{nid: 3})")
    db.cypher("create (n:person{nid: 4})")
    db.cypher("create (n:person{nid: 5})")
    db.cypher("create (n:person{nid: 6})")
    db.cypher("create (n:person{nid: 7})")
    db.cypher("create (n:person{nid: 8})")
    db.cypher("create (n:person{nid: 9})")
    db.cypher("create (n:person{nid: 1, name:'a'})")
    db.cypher("create (n:person{nid: 11})")

    db.cypher("""
                |match (n:person{nid: 1})
                |match (m:person{nid:2})
                |create (n)-[r:XXX]->(m)
                |""".stripMargin)
    db.cypher("""
                |match (n:person{nid: 1})
                |match (m:person{nid:3})
                |create (n)-[r:XXX]->(m)
                |""".stripMargin)
    db.cypher("""
                |match (n:person{nid: 1})
                |match (m:person{nid:4})
                |create (n)-[r:XXX]->(m)
                |""".stripMargin)
    db.cypher("""
                |match (n:person{nid: 2})
                |match (m:person{nid:5})
                |create (n)-[r:XXX]->(m)
                |""".stripMargin)
    db.cypher("""
                |match (n:person{nid: 2})
                |match (m:person{nid:6})
                |create (n)-[r:XXX]->(m)
                |""".stripMargin)
    db.cypher("""
                |match (n:person{nid: 6})
                |match (m:person{nid:7})
                |create (n)-[r:XXX]->(m)
                |""".stripMargin)
    db.cypher("""
                |match (n:person{nid: 3})
                |match (m:person{nid: 9})
                |create (n)-[r:XXX]->(m)
                |""".stripMargin)

    db.cypher("""
                |match (n:person{nid: 1, name:'a'})
                |match (m:person{nid:11})
                |create (n)-[r:XXX]->(m)
                |""".stripMargin)
  }
  @Test
  def testOutPath(): Unit = {
    initOutGoingExample()
    val res1 = db.cypher("match (n:person)-[r:XXX*0..3]->(m:person) return r").records()
    // 11 node + 8 hop1 + 4 hop2 + 1 hop3
    Assert.assertEquals(24, res1.size)

    // 8 hop1 + 4 hop2 + 1 hop3
    val res2 = db.cypher("match (n:person)-[r:XXX*1..3]->(m:person) return r").records()
    Assert.assertEquals(13, res2.size)

    // 4 hop2 + 1 hop3
    val res3 = db.cypher("match (n:person)-[r:XXX*2..3]->(m:person) return r").records()
    Assert.assertEquals(5, res3.size)

    // 11 nodes
    val res4 = db.cypher("match (n:person)-[r:XXX*0]->(m:person) return r").records()
    Assert.assertEquals(11, res4.size)

    // 4 hop2
    val res5 = db.cypher("match (n:person)-[r:XXX*2]->(m:person) return r").records()
    Assert.assertEquals(4, res5.size)

    // 8 hop1 + 4 hop2 + 1 hop3
    val res6 = db.cypher("match (n:person)-[r:XXX*..3]->(m:person) return r").records()
    Assert.assertEquals(13, res6.size)

    // 8 hop1 + 4 hop2 + 1 hop3
    val res7 = db.cypher("match (n:person)-[r:XXX*1..]->(m:person) return r").records()
    Assert.assertEquals(13, res7.size)
  }
  @Test
  def testInComingPath(): Unit = {
    initOutGoingExample()
    // 1 hop1 + 1 hop2 + 1 hop3
    val res = db.cypher("match (n:person{nid: 7})<-[r:XXX*1..]-(m:person) return r").records()
    Assert.assertEquals(3, res.size)

    val res1 = db.cypher("match (n:person)<-[r:XXX*0..3]-(m:person) return r").records()
    // 11 node + 8 hop1 + 4 hop2 + 1 hop3
    Assert.assertEquals(24, res1.size)

    // 8 hop1 + 4 hop2 + 1 hop3
    val res2 = db.cypher("match (n:person)<-[r:XXX*1..3]-(m:person) return r").records()
    Assert.assertEquals(13, res2.size)

    // 4 hop2 + 1 hop3
    val res3 = db.cypher("match (n:person)<-[r:XXX*2..3]-(m:person) return r").records()
    Assert.assertEquals(5, res3.size)

    // 11 nodes
    val res4 = db.cypher("match (n:person)<-[r:XXX*0]-(m:person) return r").records()
    Assert.assertEquals(11, res4.size)

    // 4 hop2
    val res5 = db.cypher("match (n:person)<-[r:XXX*2]-(m:person) return r").records()
    Assert.assertEquals(4, res5.size)

    // 8 hop1 + 4 hop2 + 1 hop3
    val res6 = db.cypher("match (n:person)<-[r:XXX*..3]-(m:person) return r").records()
    Assert.assertEquals(13, res6.size)

    // 8 hop1 + 4 hop2 + 1 hop3
    val res7 = db.cypher("match (n:person)<-[r:XXX*1..]-(m:person) return r").records()
    Assert.assertEquals(13, res7.size)
  }

  @Test
  def testOutgoingRelationshipSearch(): Unit = {
    db.cypher("""
        |create (n: Person{name:'A'})
        |create (m: Person{name:'B'})
        |create (n)-[:KNOW]->(m)
        |""".stripMargin)

    val res = db.cypher("""
        |match (n: Person{name:'A'})-[r:KNOW]->(m) where m.name = 'B' return m
        |""".stripMargin).records().next()
    Assert.assertEquals(1, res.size)
    Assert.assertEquals(
      "B",
      res("m").asInstanceOf[LynxNode].property(LynxPropertyKey("name")).get.value
    )
  }
  @Test
  def testOutgoingPathSearch(): Unit = {
    db.cypher("""
                |create (n1: Person{name:'A'})
                |create (n2: Person{name:'B'})
                |create (n3: Person{name:'C'})
                |create (n1)-[:KNOW]->(n2)
                |create (n2)-[:KNOW]->(n3)
                |""".stripMargin)

    val res = db.cypher("""
                          |match (n: Person{name:'A'})-[r:KNOW*1..3]->(m) where m.name = 'C' return m
                          |""".stripMargin).records().next()
    Assert.assertEquals(1, res.size)
    Assert.assertEquals(
      "C",
      res("m").asInstanceOf[LynxNode].property(LynxPropertyKey("name")).get.value
    )
  }

  @Test
  def testIncomingRelationshipSearch(): Unit = {
    db.cypher("""
                |create (n: Person{name:'A'})
                |create (m: Person{name:'B'})
                |create (n)<-[:KNOW]-(m)
                |""".stripMargin)

    val res = db.cypher("""
        |match (n: Person)<-[r:KNOW]-(m:Person{name:'B'})
        |where n.name = 'A' 
        |return n
        |""".stripMargin).records().next()
    Assert.assertEquals(1, res.size)
    Assert.assertEquals(
      "A",
      res("n").asInstanceOf[LynxNode].property(LynxPropertyKey("name")).get.value
    )
  }

  @Test
  def testIncomingPathSearch(): Unit = {
    db.cypher("""
                |create (n1: Person{name:'A'})
                |create (n2: Person{name:'B'})
                |create (n3: Person{name:'C'})
                |create (n1)<-[:KNOW]-(n2)
                |create (n2)<-[:KNOW]-(n3)
                |""".stripMargin)

    val res = db.cypher("""
                          |match (n: Person{name:'A'})<-[r:KNOW*1..3]-(m) where m.name = 'C' return m
                          |""".stripMargin).records().next()
    Assert.assertEquals(1, res.size)
    Assert.assertEquals(
      "C",
      res("m").asInstanceOf[LynxNode].property(LynxPropertyKey("name")).get.value
    )
  }

  @Test
  def testBothRelationshipSearch(): Unit = {
    db.cypher("""
                |create (n: Person{name:'A'})
                |create (m: Person{name:'B'})
                |create (n)<-[:KNOW]-(m)
                |create (n)-[:LOVE]->(m)
                |""".stripMargin)

    val res = db.cypher("""
                          |match (n: Person)-[r]-(m:Person)
                          |where n.name = 'A' and m.name = 'B'
                          |return n,r,m
                          |""".stripMargin).records().toList
    Assert.assertEquals(2, res.size)
    Assert.assertEquals(
      "A",
      res.head("n").asInstanceOf[LynxNode].property(LynxPropertyKey("name")).get.value
    )
    Assert.assertEquals(
      "B",
      res.last("m").asInstanceOf[LynxNode].property(LynxPropertyKey("name")).get.value
    )
  }
  @Test
  def testBothPathSearch(): Unit = {
    db.cypher("""
                |create (n1: Person{name:'A'})
                |create (n2: Person{name:'B'})
                |create (n3: Person{name:'C'})
                |create (n1)<-[:KNOW]-(n2)
                |create (n2)<-[:KNOW]-(n3)
                |""".stripMargin)

    val res = db.cypher("""
                          |match (n: Person)-[r:KNOW*1..3]-(m) where m.name = 'C' return n
                          |""".stripMargin).records().toList
    val data =
      res.map(f =>
        f("n").asInstanceOf[LynxNode].property(LynxPropertyKey("name")).get.value.toString
      )
    Assert.assertEquals(2, res.size)
    Assert.assertEquals(Seq("A", "B"), data.sorted.toSeq)
  }

  @Test
  def getPathWithoutLengthTest(): Unit = {
    initOutGoingExample()
    val res = db.cypher("match (n:person)-[r]->(m:person) return r").records()
    Assert.assertEquals(8, res.size)
  }

  @Test
  def circleTest(): Unit = {
    initHaveCircleGraphData()
    val res =
      db.cypher("match (n:person{age:1})-[r:KNOW*1..7]->(m:person{age:3}) return r").records()
    Assert.assertEquals(1, res.size)
  }

  @Test
  def testBothPath(): Unit = {
    initManualExample()
    val res1 = db
      .cypher("""
                |MATCH (charlie {name: 'Charlie Sheen'})-[r:ACTED_IN*1..3]-(movie:Movie)
                |RETURN movie.title
                |""".stripMargin)
      .records()
      .toList

    Assert.assertEquals(
      List("Wall Street", "The American President", "The American President"),
      res1.map(f => f("movie.title").value)
    )

    val res2 = db
      .cypher("""
                |MATCH (charlie {name: 'Charlie Sheen'})-[:ACTED_IN|DIRECTED*2]-(person:Person)
                |RETURN person.name
                |""".stripMargin)
      .records()
      .toList
      .map(f => f("person.name").value)
      .toSet

    Assert.assertEquals(Set("Oliver Stone", "Michael Douglas", "Martin Sheen"), res2)
  }

  @Test
  def testDetachDelete(): Unit = {
    db.cypher("create (n:person1)-[r: KNOWS]->(b:person1)")
    db.cypher("create (n:person2)-[r:K2]->(m: person2)")
    db.cypher("match (n) detach delete n")

    Assert.assertEquals(0, db.nodes().size)
    Assert.assertEquals(0, db.relationships().size)
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
      case _    => Assert.assertTrue(false)
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
      case _    => Assert.assertTrue(false)
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
      case _    => Assert.assertTrue(false)
    }
  }

  @Test
  def testStandardDateTime(): Unit = {
    val cypher = "With datetime('2022-01-01T00:00:00.000+08:00') as d Return d.year,d.quarter,d.month,d.week,d.day," +
      "d.dayOfYear,d.dayOfMonth,d.dayOfWeek,d.hour,d.minute,d.second,d.millisecond,d.microsecond,d.nanosecond,d.offset,d.epochSeconds,d.epochMillis"
    db.cypher(cypher).show()
    val result = db.cypher(cypher).records().next()
    dateTimeCheck(ZonedDateTime.parse("2022-01-01T00:00:00.000+08:00"), result)

    val cypher1 = "With datetime('2022-09-09T09:09:09.999+01:00') as d Return d.year,d.quarter,d.month,d.week,d.day," +
      "d.dayOfYear,d.dayOfMonth,d.dayOfWeek,d.hour,d.minute,d.second,d.millisecond,d.microsecond,d.nanosecond,d.offset,d.epochSeconds,d.epochMillis"
    val result1 = db.cypher(cypher1).records().next()
    dateTimeCheck(ZonedDateTime.parse("2022-09-09T09:09:09.999+01:00"), result1)
  }

  @Test
  def testSimpleDateTime(): Unit = {
    val cypher = "With datetime('2022-01-01 00:00:00 0800') as d Return d.year,d.quarter,d.month,d.week,d.day," +
      "d.dayOfYear,d.dayOfMonth,d.dayOfWeek,d.hour,d.minute,d.second,d.millisecond,d.microsecond,d.nanosecond,d.offset,d.epochSeconds,d.epochMillis"
    val result = db.cypher(cypher)
    dateTimeCheck(ZonedDateTime.parse("2022-01-01T00:00:00+08:00"), result.records().next())

    val cypher1 = "With datetime('2022-09-09 09:09:09 0100') as d Return d.year,d.quarter,d.month,d.week,d.day," +
      "d.dayOfYear,d.dayOfMonth,d.dayOfWeek,d.hour,d.minute,d.second,d.millisecond,d.microsecond,d.nanosecond,d.offset,d.epochSeconds,d.epochMillis"
    val result1 = db.cypher(cypher1)
    dateTimeCheck(ZonedDateTime.parse("2022-09-09T09:09:09+01:00"), result1.records().next())
  }

  @Test
  def testDatetimeConstructor(): Unit = {
    val cypher = "With datetime({year: 2022, month: 1, day: 1,  hour: 0, minute: 0, second: 0, nanosecond: 0,  timezone: 'Asia/Shanghai'}) as d Return d.year,d.quarter,d.month,d.week,d.day," +
      "d.dayOfYear,d.dayOfMonth,d.dayOfWeek,d.hour,d.minute,d.second,d.millisecond,d.microsecond,d.nanosecond,d.offset,d.epochSeconds,d.epochMillis"
    val result = db.cypher(cypher)
    dateTimeCheck(ZonedDateTime.parse("2022-01-01T00:00:00+08:00"), result.records().next())

    val cypher1 = "With datetime({year: 2022, month: 9, day: 9,  hour: 9, minute: 9, second: 9, nanosecond: 999999999,  timezone: 'Asia/Shanghai'}) as d Return d.year,d.quarter,d.month,d.week,d.day," +
      "d.dayOfYear,d.dayOfMonth,d.dayOfWeek,d.hour,d.minute,d.second,d.millisecond,d.microsecond,d.nanosecond,d.offset,d.epochSeconds,d.epochMillis"
    val result1 = db.cypher(cypher1)
    dateTimeCheck(
      ZonedDateTime.parse("2022-09-09T09:09:09.999999999+08:00"),
      result1.records().next()
    )
  }

  @Test
  def testNowFunction(): Unit = {
    val now = LynxDateTimeUtil.now()
    val compare = db
      .cypher("Return now() as now")
      .records()
      .next()
      .get("now")
      .get
      .asInstanceOf[LynxDateTime]
    Assert.assertTrue(now.zonedDateTime.getYear == compare.zonedDateTime.getYear)
    Assert.assertTrue(now.zonedDateTime.getMonth == compare.zonedDateTime.getMonth)
    Assert.assertTrue(now.zonedDateTime.getDayOfMonth == compare.zonedDateTime.getDayOfMonth)
    // in cross hour this case maybe fail ,retry
    Assert.assertTrue(now.zonedDateTime.getHour == compare.zonedDateTime.getHour)
    // in cross minute this case maybe fail ,retry
    Assert.assertTrue(now.zonedDateTime.getMinute == compare.zonedDateTime.getMinute)
  }

  @Test
  def timeTest(): Unit = {
    val sdf = "\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}[.]\\d{3}\\s\\d{4}"
    val time = "2002-02-02 02:02:02.111 0800"
    println(Pattern.matches(sdf, time))
  }

  @Test
  def timePatchTest: Unit = {
    val zonedDateTimeStr = "2002-02-02 02:02:02 0800"
    val arr = zonedDateTimeStr.split(" ")

    println(arr(0) + "T" + arr(1) + ".000+" + arr(2).substring(0, 2) + "." + arr(2).substring(2, 4))
    for (elem <- 1.to(12)) {
      println((elem + 2) / 3)

    }
  }

  /**
    * date result check
    * @param stringToValue
    */
  def dateTimeCheck(date: ZonedDateTime, values: Map[String, LynxValue]): Unit = {
    Assert.assertEquals(date.getYear.longValue(), values.get("d.year").get.value)
    Assert.assertEquals((date.getMonthValue.longValue() + 2) / 3, values.get("d.quarter").get.value)
    Assert.assertEquals(date.getMonthValue.longValue(), values.get("d.month").get.value)
    Assert.assertEquals(date.getDayOfYear.longValue(), values.get("d.day").get.value)
    Assert.assertEquals(date.getDayOfYear.longValue(), values.get("d.dayOfYear").get.value)
    Assert.assertEquals(date.getDayOfMonth.longValue(), values.get("d.dayOfMonth").get.value)
    Assert.assertEquals(date.getDayOfWeek.getValue.longValue(), values.get("d.dayOfWeek").get.value)
    Assert.assertEquals(date.getHour.longValue(), values.get("d.hour").get.value)
    Assert.assertEquals(date.getMinute.longValue(), values.get("d.minute").get.value)
    Assert.assertEquals(date.getSecond.longValue(), values.get("d.second").get.value)
    Assert.assertEquals(date.getNano / 100000L, values.get("d.millisecond").get.value)
    Assert.assertEquals(date.getNano / 100L, values.get("d.microsecond").get.value)
    Assert.assertEquals(date.getOffset.getId, values.get("d.offset").get.value)
    Assert.assertEquals(date.toEpochSecond.longValue(), values.get("d.epochSeconds").get.value)
    Assert.assertEquals(date.toInstant.toEpochMilli, values.get("d.epochMillis").get.value)

  }

  @Test
  def testDatetimeWeek(): Unit = {
    val cypher = "With datetime('2018-01-01 00:00:00 0800') as d Return d.week"
    val result = db.cypher(cypher).records().next()
    Assert.assertEquals(1L, result.get("d.week").get.value)

    val cypher1 = "With datetime('2019-01-01 00:00:00 0800') as d Return d.week"
    val result1 = db.cypher(cypher1).records().next()
    Assert.assertEquals(52L, result1.get("d.week").get.value)

    val cypher2 = "With datetime('2019-01-02 00:00:00 0800') as d Return d.week"
    val result2 = db.cypher(cypher2).records().next()
    Assert.assertEquals(1L, result2.get("d.week").get.value)

    val cypher3 = "With datetime('2019-01-09 00:00:00 0800') as d Return d.week"
    val result3 = db.cypher(cypher3).records().next()
    Assert.assertEquals(2L, result3.get("d.week").get.value)
  }

  @Test
  def reverseReturnSchema(): Unit = {
    db.cypher("""
        |create (p1: Person{name:'A', id:1})
        |create (p2: Person{name:'B'})
        |create (p3: Person{name:'C'})
        |create (p4: Person{name:'D'})
        |
        |create (f1: Forum{name:'F1'})
        |create (f2: Forum{name:'F2'})
        |create (f3: Forum{name:'F3'})
        |
        |create (f1)-[h1: HAS_MEMBER{joinDate:1900}]->(p2)
        |create (f2)-[h2: HAS_MEMBER{joinDate:2020}]->(p3)
        |create (f3)-[h3: HAS_MEMBER{joinDate:2030}]->(p4)
        |""".stripMargin)

    var res = db.cypher("""
        |MATCH (friend)<-[membership:HAS_MEMBER]-(forum)
        |return forum, friend
        |""".stripMargin).records().toList
    Assert.assertEquals(
      true,
      res.forall(p => p("forum").asInstanceOf[LynxNode].labels.head.value == "Forum")
    )
    Assert.assertEquals(
      true,
      res.forall(p => p("friend").asInstanceOf[LynxNode].labels.head.value == "Person")
    )
    res = db.cypher("""
                      |MATCH (friend)<-[membership:HAS_MEMBER]-(forum)
                      |return friend,forum
                      |""".stripMargin).records().toList
    Assert.assertEquals(
      true,
      res.forall(p => p("forum").asInstanceOf[LynxNode].labels.head.value == "Forum")
    )
    Assert.assertEquals(
      true,
      res.forall(p => p("friend").asInstanceOf[LynxNode].labels.head.value == "Person")
    )
  }

  @Test
  def outgoingExpandTest(): Unit = {
    db.cypher("""
                |create (n1: Person{name:'A'})
                |create (n2: Person{name:'B'})
                |create (n3: Person{name:'C'})
                |
                |create (n1)-[r1:KNOW]->(n2)
                |create (n1)-[r2:KNOW]->(n3)
                |
                |create (c1: Company{name:'Google'})
                |create (c2: Company{name:'BAT'})
                |
                |create (n2)-[r3: WORK_AT]->(c1)
                |create (n3)-[r3: WORK_AT]->(c2)
                |""".stripMargin)
    val res = db
      .cypher(
        "match (n: Person)-[r: KNOW]->(m: Person)-[r2: WORK_AT]->(c: Company{name:'BAT'}) return m"
      )
      .records()
      .toList
    Assert.assertEquals(1, res.size)
    Assert.assertEquals(
      "C",
      res.head("m").asInstanceOf[LynxNode].property(LynxPropertyKey("name")).get.value
    )
  }

  @Test
  def incomingExpandTest(): Unit = {
    db.cypher("""
                |create (n1: Person{name:'A'})
                |create (n2: Person{name:'B'})
                |create (n3: Person{name:'C'})
                |
                |create (n1)-[r1:KNOW]->(n2)
                |create (n1)-[r2:KNOW]->(n3)
                |
                |create (c1: Company{name:'Google'})
                |create (c2: Company{name:'BAT'})
                |
                |create (n2)<-[r3: Fire]-(c1)
                |create (n3)<-[r3: Fire]-(c2)
                |""".stripMargin)
    val res = db
      .cypher(
        "match (n: Person)-[r: KNOW]->(m: Person)<-[r2: Fire]-(c: Company{name:'BAT'}) return m"
      )
      .records()
      .toList
    Assert.assertEquals(1, res.size)
    Assert.assertEquals(
      "C",
      res.head("m").asInstanceOf[LynxNode].property(LynxPropertyKey("name")).get.value
    )
  }

  @Test
  def bothExpandTest(): Unit = {
    db.cypher("""
                |create (n1: Person{name:'A'})
                |create (n2: Person{name:'B'})
                |create (n3: Person{name:'C'})
                |
                |create (n1)-[r1:KNOW]->(n2)
                |create (n1)-[r2:KNOW]->(n3)
                |
                |create (c1: Company{name:'Google'})
                |create (c2: Company{name:'BAT'})
                |
                |create (n2)-[r3: RELATED]->(c2)
                |create (n3)<-[r3: RELATED]-(c2)
                |""".stripMargin)
    val res = db
      .cypher(
        "match (n: Person)-[r: KNOW]->(m: Person)-[r2: Fire]-(c: Company{name:'BAT'}) return m"
      )
      .records()
      .toList
    Assert.assertEquals(2, res.size)
    Assert.assertEquals(
      "B",
      res.head("m").asInstanceOf[LynxNode].property(LynxPropertyKey("name")).get.value
    )
    Assert.assertEquals(
      "C",
      res.last("m").asInstanceOf[LynxNode].property(LynxPropertyKey("name")).get.value
    )
  }

}
