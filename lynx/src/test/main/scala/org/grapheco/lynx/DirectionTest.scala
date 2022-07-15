package org.grapheco.lynx

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey, LynxRelationshipType}
import org.junit.{Assert, Test}

/**
  *@author:John117
  *@createDate:2022/7/14
  *@description:
  */
class DirectionTest extends TestBase {

  def checkARSCorrect(res: List[Map[String, LynxValue]]): Unit = {
    Assert.assertEquals(3, res.size)
    Assert.assertEquals(
      Map(
        "a" -> TestNode(
          TestId(2),
          Seq(LynxNodeLabel("Album")),
          Map(LynxPropertyKey("name") -> LynxValue("Let Go"))
        ),
        "r" -> TestRelationship(
          TestId(1),
          TestId(1),
          TestId(2),
          Option(LynxRelationshipType("CREATE")),
          Map(LynxPropertyKey("year") -> LynxValue(2002))
        ),
        "s" -> TestNode(
          TestId(1),
          Seq(LynxNodeLabel("Singer")),
          Map(LynxPropertyKey("name") -> LynxValue("Avril Lavigne"))
        )
      ),
      res(0)
    )
  }
  def checkSRACorrect(res: List[Map[String, LynxValue]]): Unit = {
    Assert.assertEquals(3, res.size)
    Assert.assertEquals(
      Map(
        "s" -> TestNode(
          TestId(1),
          Seq(LynxNodeLabel("Singer")),
          Map(LynxPropertyKey("name") -> LynxValue("Avril Lavigne"))
        ),
        "r" -> TestRelationship(
          TestId(1),
          TestId(1),
          TestId(2),
          Option(LynxRelationshipType("CREATE")),
          Map(LynxPropertyKey("year") -> LynxValue(2002))
        ),
        "a" -> TestNode(
          TestId(2),
          Seq(LynxNodeLabel("Album")),
          Map(LynxPropertyKey("name") -> LynxValue("Let Go"))
        )
      ),
      res(0)
    )
  }

  @Test
  def testCreateOutgoingSingleRelationshipWithDifferentNodeLabel(): Unit = {
    runOnDemoGraph(
      """
        |create (n:Singer{name:'Avril Lavigne'}), (m: Album{name:'Let Go'}), (u: Album{name:'Under My Skin'}), (t: Album{name: 'The Best Damn Thing'})
        |create (n)-[r1:CREATE{year:2002}]->(m)
        |create (n)-[r2:CREATE{year:2004}]->(u)
        |create (n)-[r3:CREATE{year:2008}]->(t)
        |""".stripMargin
    )
    // outgoing
    var res = runOnDemoGraph("match (s:Singer)-[r:CREATE]->(a:Album) return s,r,a").records().toList
    checkSRACorrect(res)
    res = runOnDemoGraph("match (s:Singer)-[r:CREATE]->(a:Album) return a,r,s").records().toList
    checkARSCorrect(res)

    // incoming
    res = runOnDemoGraph("match (a:Album)<-[r:CREATE]-(s:Singer) return a,r,s").records().toList
    checkARSCorrect(res)
    res = runOnDemoGraph("match (a:Album)<-[r:CREATE]-(s:Singer) return s,r,a").records().toList
    checkSRACorrect(res)

    // both
    res = runOnDemoGraph("match (s:Singer)-[r:CREATE]-(a:Album) return s,r,a").records().toList
    checkSRACorrect(res)
    res = runOnDemoGraph("match (s:Singer)-[r:CREATE]-(a:Album) return a,r,s").records().toList
    checkARSCorrect(res)
    res = runOnDemoGraph("match (a:Album)-[r:CREATE]-(s:Singer) return s,r,a").records().toList
    checkSRACorrect(res)
    res = runOnDemoGraph("match (a:Album)-[r:CREATE]-(s:Singer) return a,r,s").records().toList
    checkARSCorrect(res)
  }

  @Test
  def testCreateIncomingSingleRelationshipWithDifferentNodeLabel(): Unit = {
    runOnDemoGraph(
      """
      |create (n:Singer{name:'Avril Lavigne'}), (m: Album{name:'Let Go'}), (u: Album{name:'Under My Skin'}), (t: Album{name: 'The Best Damn Thing'})
      |create (m)<-[r1:CREATE{year:2002}]-(n)
      |create (u)<-[r2:CREATE{year:2004}]-(n)
      |create (t)<-[r3:CREATE{year:2008}]-(n)
      |""".stripMargin
    )

    // outgoing
    var res = runOnDemoGraph("match (s:Singer)-[r:CREATE]->(a:Album) return s,r,a").records().toList
    checkSRACorrect(res)
    res = runOnDemoGraph("match (s:Singer)-[r:CREATE]->(a:Album) return a,r,s").records().toList
    checkARSCorrect(res)

    // incoming
    res = runOnDemoGraph("match (a:Album)<-[r:CREATE]-(s:Singer) return a,r,s").records().toList
    checkARSCorrect(res)
    res = runOnDemoGraph("match (a:Album)<-[r:CREATE]-(s:Singer) return s,r,a").records().toList
    checkSRACorrect(res)

    // both
    res = runOnDemoGraph("match (s:Singer)-[r:CREATE]-(a:Album) return s,r,a").records().toList
    checkSRACorrect(res)
    res = runOnDemoGraph("match (s:Singer)-[r:CREATE]-(a:Album) return a,r,s").records().toList
    checkARSCorrect(res)
    res = runOnDemoGraph("match (a:Album)-[r:CREATE]-(s:Singer) return s,r,a").records().toList
    checkSRACorrect(res)
    res = runOnDemoGraph("match (a:Album)-[r:CREATE]-(s:Singer) return a,r,s").records().toList
    checkARSCorrect(res)
  }

  @Test
  def testCreateOutgoingSingleRelationshipWithSameNodeLabel(): Unit = {
    runOnDemoGraph(
      """
        |create (s1:Singer{name:'Avril Lavigne'}), (s2: Singer{name:'Tylor Swift'}), (s3: Singer{name:'Michael Jackson'})
        |create (s1)-[r1:KNOW]->(s2)
        |create (s1)-[r2:KNOW]->(s3)
        |create (s2)-[r3:KNOW]->(s3)
        |""".stripMargin
    )

    var res =
      runOnDemoGraph("match (n: Singer)-[r:KNOW]->(m: Singer) return n,r,m").records().toList
    Assert.assertEquals(3, res.size)
    res = runOnDemoGraph("match (n: Singer)<-[r:KNOW]-(m: Singer) return n,r,m").records().toList
    Assert.assertEquals(3, res.size)
    res = runOnDemoGraph("match (n: Singer)-[r:KNOW]-(m: Singer) return n,r,m").records().toList
    Assert.assertEquals(6, res.size)
  }

  @Test
  def testCreateIncomingSingleRelationshipWithSameNodeLabel(): Unit = {
    runOnDemoGraph(
      """
        |create (s1:Singer{name:'Avril Lavigne'}), (s2: Singer{name:'Tylor Swift'}), (s3: Singer{name:'Michael Jackson'})
        |create (s1)<-[r1:KNOW]-(s2)
        |create (s1)<-[r2:KNOW]-(s3)
        |create (s2)<-[r3:KNOW]-(s3)
        |""".stripMargin
    )

    var res =
      runOnDemoGraph("match (n: Singer)-[r:KNOW]->(m: Singer) return n,r,m").records().toList
    Assert.assertEquals(3, res.size)
    res = runOnDemoGraph("match (n: Singer)<-[r:KNOW]-(m: Singer) return n,r,m").records().toList
    Assert.assertEquals(3, res.size)
    res = runOnDemoGraph("match (n: Singer)-[r:KNOW]-(m: Singer) return n,r,m").records().toList
    Assert.assertEquals(6, res.size)
  }
}
