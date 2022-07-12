package org.grapheco.tudb.ldbctest

import org.apache.commons.io.FileUtils
import org.grapheco.tudb.ldbctest.ComplexQueryTest.db
import org.grapheco.tudb.{GraphDatabaseBuilder, TuDBInstanceContext}
import org.grapheco.tudb.test.TestUtils
import org.junit.{After, AfterClass, Assert, Test}

import java.io.File

object ComplexQueryTest {
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

/**
  *@author:John117
  *@createDate:2022/7/4
  *@description:
  */
class ComplexQueryTest {
  @After
  def clean(): Unit = {
    db.cypher("match (n) detach delete n")
  }
  @Test
  def Q2(): Unit = {
    db.cypher(
      """
        |create (n: Person{id: 1})
        |create (m: Person{id: 2, firstName:'AAA', lastName:'BBB'})
        |create (q1: Message{id:3, creationDate: '2001-01-01 00:00:00.000', content: 'My Content', imageFile: 'iiiii'})
        |create (q2: Message{id:4, creationDate: '2021-01-01 00:00:00.000', imageFile: 'My ImageFile'})
        |""".stripMargin
    )
    db.cypher("""
        |match (n: Person{id: 1})
        |match (m: Person{id: 2})
        |create (n)-[r: KNOWS]-(m)
        |""".stripMargin)
    db.cypher("""
        |match (n: Person{id: 2})
        |match (m: Message{id: 3})
        |create (n)<-[r: HAS_CREATOR]-(m)
        |""".stripMargin)
    db.cypher("""
        |match (n: Person{id: 2})
        |match (m: Message{id: 4})
        |create (n)<-[r: HAS_CREATOR]-(m)
        |""".stripMargin)

    val res = db.cypher(s"""
                      |MATCH (:Person {id: 1 })-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(message:Message)
                      |    WHERE message.creationDate <= '2011'
                      |    RETURN
                      |        friend.id AS personId,
                      |        friend.firstName AS personFirstName,
                      |        friend.lastName AS personLastName,
                      |        message.id AS postOrCommentId,
                      |        coalesce(message.content,message.imageFile) AS postOrCommentContent,
                      |        message.creationDate AS postOrCommentCreationDate
                      |    ORDER BY
                      |        postOrCommentCreationDate DESC,
                      |        toInteger(postOrCommentId) ASC
                      |    LIMIT 20
                      |""".stripMargin).records().toList

    Assert.assertEquals(2, res.size)
    Assert.assertEquals(Seq("3", "3"), res.map(mp => mp("postOrCommentId").value.toString))
  }
  @Test
  def Q3(): Unit = {
    db.cypher("""
        |create (n1: Person{name:'A', id: 1})
        |create (n11: Person{name:'B'})
        |create (n12: Person{firstName:'FFF', lastName:'LLL', id:666})
        |
        |create (n2: Country{name:'China'})
        |create (n3: Country{name:'USA'})
        |create (n4: Country{name:'Japan'})
        |
        |create (n5: City{name:'Chengdu'})
        |create (n6: City{name: 'Beijing'})
        |create (n7: City{name:'Tokyo'})
        |
        |create (n5)-[r1:IS_PART_OF]->(n2)
        |create (n6)-[r2:IS_PART_OF]->(n2)
        |create (n7)-[r3:IS_PART_OF]->(n4)
        |
        |create (n1)-[r4:KNOWS]->(n11)
        |create (n11)-[r5:KNOWS]->(n12)
        |
        |create (n1)-[: IS_LOCATED_IN]->(n5)
        |create (n11)-[: IS_LOCATED_IN]->(n6)
        |create (n12)-[: IS_LOCATED_IN]->(n7)
        |
        |create (m1: Message{creationDate:2000})
        |create (m2: Message{creationDate:2010})
        |create (m3: Message{creationDate:2020})
        |
        |create (n12)<-[hc1: HAS_CREATOR]-(m1)
        |create (n12)<-[hc2: HAS_CREATOR]-(m2)
        |create (n12)<-[hc3: HAS_CREATOR]-(m3)
        |
        |create (m1)-[i1: IS_LOCATED_IN]->(n2)
        |create (m2)-[i2: IS_LOCATED_IN]->(n3)
        |create (m3)-[i3: IS_LOCATED_IN]->(n4)
        |""".stripMargin)

    val res = db.cypher(s"""
        |MATCH (countryX:Country {name: 'China' }),
        |      (countryY:Country {name: 'USA' }),
        |      (person:Person {id: 1 })
        |WITH person, countryX, countryY
        |LIMIT 1
        |MATCH (city:City)-[:IS_PART_OF]->(country:Country)
        |WHERE country IN [countryX, countryY]
        |WITH person, countryX, countryY, collect(city) AS cities
        |MATCH (person)-[:KNOWS*1..2]-(friend)-[:IS_LOCATED_IN]->(city)
        |WHERE NOT person=friend AND NOT city IN cities
        |WITH DISTINCT friend, countryX, countryY
        |MATCH (friend)<-[:HAS_CREATOR]-(message),
        |      (message)-[:IS_LOCATED_IN]->(country)
        |WHERE 3000 > message.creationDate >= 1000 AND
        |      country IN [countryX, countryY]
        |WITH friend,
        |     CASE WHEN country=countryX THEN 1 ELSE 0 END AS messageX,
        |     CASE WHEN country=countryY THEN 1 ELSE 0 END AS messageY
        |WITH friend, sum(messageX) AS xCount, sum(messageY) AS yCount
        |WHERE xCount>0 AND yCount>0
        |RETURN friend.id AS friendId,
        |       friend.firstName AS friendFirstName,
        |       friend.lastName AS friendLastName,
        |       xCount,
        |       yCount,
        |       xCount + yCount AS xyCount
        |ORDER BY xyCount DESC, friendId ASC
        |LIMIT 20
        |""".stripMargin).records().next()
    Assert.assertEquals(666L, res("friendId").value)
    Assert.assertEquals("FFF", res("friendFirstName").value)
    Assert.assertEquals("LLL", res("friendLastName").value)
    Assert.assertEquals(1.0, res("xCount").value)
    Assert.assertEquals(1.0, res("yCount").value)
    Assert.assertEquals(2.0, res("xyCount").value)
  }

  @Test
  def Q4(): Unit = {
    db.cypher(
      """
        |create (n: Person{id: 1, name: 'AAA'})
        |create (m: Person{id: 2, name:'BBB'})
        |create (p1: Post{id:3, creationDate: '2010'})
        |create (p2: Post{id:4, creationDate: '2020'})
        |create (t1: Tag{name:'T1'})
        |create (t2: Tag{name:'T2'})
        |
        |create (n)-[r1:KNOWS]->(m)
        |create (m)<-[r2: HAS_CREATOR]-(p1)
        |create (m)<-[r3: HAS_CREATOR]-(p2)
        |create (p1)-[r4:HAS_TAG]->(t1)
        |create (p1)-[r5:HAS_TAG]->(t2)
        |create (p2)-[r6:HAS_TAG]->(t1)
        |create (p2)-[r7:HAS_TAG]->(t2)
        |""".stripMargin
    )

    val res = db.cypher(s"""
                           |MATCH (person:Person {id: 1 })-[:KNOWS]-(friend:Person),
                           |      (friend)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(tag)
                           |WITH DISTINCT tag, post
                           |WITH tag,
                           |     CASE
                           |       WHEN '3000' > post.creationDate >= '2000' THEN 1
                           |       ELSE 0
                           |     END AS valid,
                           |     CASE
                           |       WHEN '3000' < post.creationDate THEN 1
                           |       ELSE 0
                           |     END AS inValid
                           |WITH tag, sum(valid) AS postCount, sum(inValid) AS inValidPostCount
                           |WHERE postCount>0 AND inValidPostCount=0
                           |return tag.name as tagName, postCount, inValidPostCount
                           |ORDER BY postCount DESC, tagName ASC
                           |LIMIT 10
                           |""".stripMargin).records().toList

    Assert.assertEquals(2, res.size)
    Assert.assertEquals("T1", res.head("tagName").value)
    Assert.assertEquals(2.0, res.head("postCount").value)
    Assert.assertEquals(0.0, res.head("inValidPostCount").value)
    Assert.assertEquals("T2", res.last("tagName").value)
    Assert.assertEquals(2.0, res.last("postCount").value)
    Assert.assertEquals(0.0, res.last("inValidPostCount").value)
  }
}
