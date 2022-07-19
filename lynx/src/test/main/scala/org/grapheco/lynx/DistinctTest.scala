package org.grapheco.lynx

import org.junit.{Assert, Test}

/**
  *@author:John117
  *@createDate:2022/7/18
  *@description:
  */
class DistinctTest extends TestBase {
  @Test
  def testDistinctRelationship(): Unit = {
    runOnDemoGraph("""
        |create (n1:Person{name:'A'})
        |create (n2:Person{name:'B'})
        |create (n3:Person{name:'C'})
        |create (n1)-[:KNOW]->(n3)
        |create (n2)-[:KNOW]->(n3)
        |""".stripMargin)

    var count = runOnDemoGraph("""
        |match (n)-[r:KNOW]->(m)
        |return count(m) as count
        |""".stripMargin).records().next()
    Assert.assertEquals(2L, count("count").value)

    count = runOnDemoGraph("""
        |match (n)-[r:KNOW]->(m)
        |with distinct m
        |return count(m) as count
        |""".stripMargin).records().next()
    Assert.assertEquals(1L, count("count").value)

    count = runOnDemoGraph("""
        |match (n)-[r:KNOW]->(m)
        |return count(m) as count
        |""".stripMargin).records().next()
    Assert.assertEquals(2L, count("count").value)

    count = runOnDemoGraph("""
        |match (n)-[r:KNOW]->(m)
        |return count(distinct m) as count
        |""".stripMargin).records().next()
    Assert.assertEquals(1L, count("count").value)
  }
}
