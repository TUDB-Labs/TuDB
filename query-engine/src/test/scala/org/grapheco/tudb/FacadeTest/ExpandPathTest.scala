package org.grapheco.tudb.FacadeTest

import org.grapheco.lynx.types.structural.{LynxNode, LynxPropertyKey}
import org.grapheco.tudb.FacadeTest.GraphFacadeTest.db
import org.junit.{Assert, Test}

/**
  *@author:John117
  *@createDate:2022/7/25
  *@description:
  */
class ExpandPathTest extends GraphFacadeTest {
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
        "match (n: Person)-[r: KNOW]->(m: Person)-[r2: RELATED]-(c: Company{name:'BAT'}) return m"
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

  @Test
  def expandWithLengthOutgoingTest(): Unit = {
    db.cypher("""
        |create (n1:Person{name:'A'})
        |create (n2: Person{name:'B'})
        |create (n1)-[r1:KNOW]->(n2)
        |
        |create (n3: Person{name:'C'})
        |create (n4: Person{name:'D'})
        |create (n5: Person{name:'E'})
        |create (n6: Person{name:'F'})
        |create (n2)-[r2:LOVE]->(n3)
        |create (n2)-[r3:LOVE]->(n4)
        |create (n3)-[r4:LOVE]->(n4)
        |create (n4)-[r5:LOVE]->(n5)
        |create (n5)-[r5:LOVE]->(n6)
        |""".stripMargin)
    val record =
      db.cypher("""match (n)-[r:KNOW]->(m)-[r2:LOVE*1..2]->(q) return r2""").records().toList
    Assert.assertEquals(4, record.size)
  }

  @Test
  def expandWithLengthIncomingTest(): Unit = {
    db.cypher("""
                |create (n1:Person{name:'A'})
                |create (n2: Person{name:'B'})
                |create (n1)-[r1:KNOW]->(n2)
                |
                |create (n3: Person{name:'C'})
                |create (n4: Person{name:'D'})
                |create (n5: Person{name:'E'})
                |create (n6: Person{name:'F'})
                |create (n2)<-[r2:LOVE]-(n3)
                |create (n2)<-[r3:LOVE]-(n4)
                |create (n3)<-[r4:LOVE]-(n4)
                |create (n4)<-[r5:LOVE]-(n5)
                |create (n5)<-[r5:LOVE]-(n6)
                |""".stripMargin)
    val record =
      db.cypher("""match (n)-[r:KNOW]->(m)<-[r2:LOVE*1..2]-(q) return r2""").records().toList
    Assert.assertEquals(4, record.size)
  }

  @Test
  def expandWithLengthBothTest(): Unit = {
    db.cypher("""
                |create (n1:Person{name:'A'})
                |create (n2: Person{name:'B'})
                |create (n1)-[r1:KNOW]->(n2)
                |
                |create (n3: Person{name:'C'})
                |create (n4: Person{name:'D'})
                |create (n5: Person{name:'E'})
                |create (n6: Person{name:'F'})
                |create (n2)-[r2:LOVE]->(n3)
                |create (n2)-[r3:LOVE]->(n4)
                |create (n3)-[r4:LOVE]->(n4)
                |create (n4)-[r5:LOVE]->(n5)
                |create (n5)-[r6:LOVE]->(n6)
                |""".stripMargin)
    val record =
      db.cypher("""match (n)-[r:KNOW]->(m)-[r2:LOVE*1..2]-(q) return r2""").records().toList
    Assert.assertEquals(5, record.size)
  }
}
