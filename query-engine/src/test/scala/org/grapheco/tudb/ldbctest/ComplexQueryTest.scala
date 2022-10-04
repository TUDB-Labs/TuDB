package org.grapheco.tudb.ldbctest

import org.apache.commons.io.FileUtils
import org.grapheco.lynx.types.property.{LynxNull, LynxString}
import org.grapheco.tudb.ldbctest.ComplexQueryTest.db
import org.grapheco.tudb.store.node.TuNode
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
        |create (n)-[r: KNOWS]->(m)
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

    Assert.assertEquals(1, res.size)
    Assert.assertEquals(Seq("3"), res.map(mp => mp("postOrCommentId").value.toString))
  }
//  @Test
//  def Q3(): Unit = {
//    db.cypher("""
//        |create (n1: Person{name:'A', id: 1})
//        |create (n11: Person{name:'B'})
//        |create (n12: Person{firstName:'FFF', lastName:'LLL', id:666})
//        |
//        |create (n2: Country{name:'China'})
//        |create (n3: Country{name:'USA'})
//        |create (n4: Country{name:'Japan'})
//        |
//        |create (n5: City{name:'Chengdu'})
//        |create (n6: City{name: 'Beijing'})
//        |create (n7: City{name:'Tokyo'})
//        |
//        |create (n5)-[r1:IS_PART_OF]->(n2)
//        |create (n6)-[r2:IS_PART_OF]->(n2)
//        |create (n7)-[r3:IS_PART_OF]->(n4)
//        |
//        |create (n1)-[r4:KNOWS]->(n11)
//        |create (n11)-[r5:KNOWS]->(n12)
//        |
//        |create (n1)-[: IS_LOCATED_IN]->(n5)
//        |create (n11)-[: IS_LOCATED_IN]->(n6)
//        |create (n12)-[: IS_LOCATED_IN]->(n7)
//        |
//        |create (m1: Message{creationDate:2000})
//        |create (m2: Message{creationDate:2010})
//        |create (m3: Message{creationDate:2020})
//        |
//        |create (n12)<-[hc1: HAS_CREATOR]-(m1)
//        |create (n12)<-[hc2: HAS_CREATOR]-(m2)
//        |create (n12)<-[hc3: HAS_CREATOR]-(m3)
//        |
//        |create (m1)-[i1: IS_LOCATED_IN]->(n2)
//        |create (m2)-[i2: IS_LOCATED_IN]->(n3)
//        |create (m3)-[i3: IS_LOCATED_IN]->(n4)
//        |""".stripMargin)
//
//    val res = db.cypher(s"""
//        |MATCH (countryX:Country {name: 'China' }),
//        |      (countryY:Country {name: 'USA' }),
//        |      (person:Person {id: 1 })
//        |WITH person, countryX, countryY
//        |LIMIT 1
//        |MATCH (city:City)-[:IS_PART_OF]->(country:Country)
//        |WHERE country IN [countryX, countryY]
//        |WITH person, countryX, countryY, collect(city) AS cities
//        |MATCH (person)-[:KNOWS*1..2]-(friend)-[:IS_LOCATED_IN]->(city)
//        |WHERE NOT person=friend AND NOT city IN cities
//        |WITH DISTINCT friend, countryX, countryY
//        |MATCH (friend)<-[:HAS_CREATOR]-(message),
//        |      (message)-[:IS_LOCATED_IN]->(country)
//        |WHERE 3000 > message.creationDate >= 1000 AND
//        |      country IN [countryX, countryY]
//        |WITH friend,
//        |     CASE WHEN country=countryX THEN 1 ELSE 0 END AS messageX,
//        |     CASE WHEN country=countryY THEN 1 ELSE 0 END AS messageY
//        |WITH friend, sum(messageX) AS xCount, sum(messageY) AS yCount
//        |WHERE xCount>0 AND yCount>0
//        |RETURN friend.id AS friendId,
//        |       friend.firstName AS friendFirstName,
//        |       friend.lastName AS friendLastName,
//        |       xCount,
//        |       yCount,
//        |       xCount + yCount AS xyCount
//        |ORDER BY xyCount DESC, friendId ASC
//        |LIMIT 20
//        |""".stripMargin).records().next()
//    Assert.assertEquals(666L, res("friendId").value)
//    Assert.assertEquals("FFF", res("friendFirstName").value)
//    Assert.assertEquals("LLL", res("friendLastName").value)
//    Assert.assertEquals(1.0, res("xCount").value)
//    Assert.assertEquals(1.0, res("yCount").value)
//    Assert.assertEquals(2.0, res("xyCount").value)
//  }
//
//  @Test
//  def Q4(): Unit = {
//    db.cypher(
//      """
//        |create (n: Person{id: 1, name: 'AAA'})
//        |create (m: Person{id: 2, name:'BBB'})
//        |create (p1: Post{id:3, creationDate: '2010'})
//        |create (p2: Post{id:4, creationDate: '2020'})
//        |create (t1: Tag{name:'T1'})
//        |create (t2: Tag{name:'T2'})
//        |
//        |create (n)-[r1:KNOWS]->(m)
//        |create (m)<-[r2: HAS_CREATOR]-(p1)
//        |create (m)<-[r3: HAS_CREATOR]-(p2)
//        |create (p1)-[r4:HAS_TAG]->(t1)
//        |create (p1)-[r5:HAS_TAG]->(t2)
//        |create (p2)-[r6:HAS_TAG]->(t1)
//        |create (p2)-[r7:HAS_TAG]->(t2)
//        |""".stripMargin
//    )
//
//    val res = db.cypher(s"""
//                           |MATCH (person:Person {id: 1 })-[:KNOWS]-(friend:Person),
//                           |      (friend)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(tag)
//                           |WITH DISTINCT tag, post
//                           |WITH tag,
//                           |     CASE
//                           |       WHEN '3000' > post.creationDate >= '2000' THEN 1
//                           |       ELSE 0
//                           |     END AS valid,
//                           |     CASE
//                           |       WHEN '3000' < post.creationDate THEN 1
//                           |       ELSE 0
//                           |     END AS inValid
//                           |WITH tag, sum(valid) AS postCount, sum(inValid) AS inValidPostCount
//                           |WHERE postCount>0 AND inValidPostCount=0
//                           |return tag.name as tagName, postCount, inValidPostCount
//                           |ORDER BY postCount DESC, tagName ASC
//                           |LIMIT 10
//                           |""".stripMargin).records().toList
//
//    Assert.assertEquals(2, res.size)
//    Assert.assertEquals("T1", res.head("tagName").value)
//    Assert.assertEquals(2.0, res.head("postCount").value)
//    Assert.assertEquals(0.0, res.head("inValidPostCount").value)
//    Assert.assertEquals("T1", res.head("tagName").value)
//    Assert.assertEquals(2.0, res.last("postCount").value)
//    Assert.assertEquals(0.0, res.last("inValidPostCount").value)
//  }
//  @Test
//  def Q5(): Unit = {
//
//    db.cypher(
//      """
//        |create (n: Person{id: 1, name: 'AAA'})
//        |create (m: Person{id: 2, name:'BBB'})
//        |create (p1: Post{id:3, creationDate: 2010})
//        |create (p2: Post{id:4, creationDate: 2020})
//        |create (f1: Forum{name:'T1'})
//        |create (f2: Forum{name:'T2'})
//        |
//        |create (n)-[r1:KNOWS]->(m)
//        |create (m)<-[r2: HAS_CREATOR]-(p1)
//        |create (m)<-[r3: HAS_CREATOR]-(p2)
//        |create (f1)-[r4:HAS_MEMBER{joinDate:2021}]->(m)
//        |create (f2)-[r5:HAS_MEMBER{joinDate:2022}]->(m)
//        |""".stripMargin
//    )
//    val res = db.cypher(s"""
//       |MATCH (person:Person { id: 1 })-[:KNOWS*1..2]-(friend)
//       |WHERE  NOT person=friend
//       |WITH DISTINCT friend,
//       |    collect(friend) AS friends
//       |
//       |MATCH (fri)<-[:HAS_CREATOR]-(post)
//       |WHERE
//       |    fri IN friends
//       |return post
//       |""".stripMargin).records().toList
//    println(res)
//    Assert.assertEquals(2, res.size)
//    Assert.assertEquals(
//      2010L,
//      res.head("post").asInstanceOf[TuNode].property("creationDate").get.value
//    )
//    Assert.assertEquals(
//      2020L,
//      res.last("post").asInstanceOf[TuNode].property("creationDate").get.value
//    )
//  }
//
//  @Test
//  def Q9(): Unit = {
//    db.cypher("""
//        |create (n1:Person{firstName:'A1', lastName:'A2', id: 1})
//        |create (n2:Person{firstName:'B1', lastName:'B2', id: 2})
//        |create (n3:Person{firstName:'C1', lastName:'C2', id: 3})
//        |create (n4:Person{firstName:'D1', lastName:'D2', id: 4})
//        |
//        |create (n1)-[:KNOWS]->(n2)
//        |create (n1)-[:KNOWS]->(n4)
//        |create (n2)-[:KNOWS]->(n3)
//        |
//        |create (m1:Message{id:10, content:'hahahahaha', creationDate:2019})
//        |create (m2:Message{id:11, imageFile:'pic.baidu.com', creationDate:2029})
//        |create (m3:Message{id:12, content:'wowowowowowo', creationDate:2009})
//        |
//        |create (n2)<-[:HAS_CREATOR]-(m1)
//        |create (n3)<-[:HAS_CREATOR]-(m2)
//        |create (n4)<-[:HAS_CREATOR]-(m3)
//        |""".stripMargin)
//
//    val res = db.cypher(s"""
//                 |MATCH (root:Person {id: 1 })-[:KNOWS*1..2]-(friend:Person)
//                 |WITH collect(distinct friend) as friends
//                 |UNWIND friends as friend
//                 |    MATCH (friend)<-[:HAS_CREATOR]-(message:Message)
//                 |    WHERE message.creationDate < 2020
//                 |RETURN
//                 |    friend.id AS personId,
//                 |    friend.firstName AS personFirstName,
//                 |    friend.lastName AS personLastName,
//                 |    message.id AS commentOrPostId,
//                 |    coalesce(message.content,message.imageFile) AS commentOrPostContent,
//                 |    message.creationDate AS commentOrPostCreationDate
//                 |ORDER BY
//                 |    commentOrPostCreationDate DESC,
//                 |    message.id ASC
//                 |LIMIT 20
//                 |""".stripMargin).records().toList
//
//    val data1 = res.map(f => f("commentOrPostCreationDate").value.toString)
//    Assert.assertEquals(Seq("2019", "2009"), data1)
//
//    val data2 = res.map(f => f("commentOrPostId").value.toString)
//    Assert.assertEquals(Seq("10", "12"), data2)
//  }

  @Test
  def Q11(): Unit = {
    db.cypher("""
        |create (n1:Person{firstName:'A1', lastName:'A2', id: 1})
        |create (n2:Person{firstName:'B1', lastName:'B2', id: 2})
        |create (n3:Person{firstName:'C1', lastName:'C2', id: 3})
        |create (n4:Person{firstName:'D1', lastName:'D2', id: 4})
        |
        |create (n1)-[:KNOWS]->(n2)
        |create (n1)-[:KNOWS]->(n4)
        |create (n2)-[:KNOWS]->(n3)
        |
        |create (c1: Company{name:'Google'})
        |create (c2: Company{name:'Facebook'})
        |create (c3: Company{name:'Amazon'})
        |create (c4: Country{name:'China'})
        |create (c5: Country{name:'USA'})
        |
        |create (n2)-[:WORK_AT{workFrom:2010}]->(c1)
        |create (n3)-[:WORK_AT{workFrom:2020}]->(c2)
        |create (n4)-[:WORK_AT{workFrom:2030}]->(c3)
        |
        |create (c1)-[:IS_LOCATED_IN]->(c4)
        |create (c2)-[:IS_LOCATED_IN]->(c5)
        |create (c3)-[:IS_LOCATED_IN]->(c5)
        |""".stripMargin)

    val res = db.cypher(s"""
                 |MATCH (person:Person {id: 1 })-[:KNOWS*1..2]-(friend:Person)
                 |WHERE not(person=friend)
                 |WITH DISTINCT friend
                 |MATCH (friend)-[workAt:WORK_AT]->(company:Company)-[:IS_LOCATED_IN]->(:Country {name: 'China' })
                 |WHERE workAt.workFrom < 2022
                 |RETURN
                 |        friend.id AS personId,
                 |        friend.firstName AS personFirstName,
                 |        friend.lastName AS personLastName,
                 |        company.name AS organizationName,
                 |        workAt.workFrom AS organizationWorkFromYear
                 |ORDER BY
                 |        organizationWorkFromYear ASC,
                 |        toInteger(personId) ASC,
                 |        organizationName DESC
                 |LIMIT 10
                 |""".stripMargin).records().toList

    Assert.assertEquals(1, res.size)
    Assert.assertEquals(2L, res.head("personId").value)
  }

//  @Test
//  def Q12(): Unit = {
//    db.cypher("""
//        |create (n1:Person{name:'P1', id: 1})
//        |create (n2:Person{name:'P2', id: 2})
//        |create (c1:Comment{name:'C1'})
//        |create (p1: Post{name:'P1'})
//        |
//        |create (n1)-[:KNOWS]->(n2)
//        |create (n2)<-[:HAS_CREATOR]-(c1)
//        |create (c1)-[:REPLY_OF]->(p1)
//        |
//        |create (t1: Tag{name:'A', id: 1})
//        |create (t2: Tag{name:'T2', id: 2})
//        |create (t3: Tag{name:'T3', id: 3})
//        |create (b1: TagClass{name:'TC1'})
//        |create (b2: TagClass{name:'B', id:4})
//        |
//        |create (t1)-[r:HAS_TYPE]->(b1)
//        |create (t2)-[r:HAS_TYPE]->(b1)
//        |create (t3)-[r:IS_SUBCLASS_OF]->(b2)
//        |
//        |create (p1)-[:HAS_TAG]->(t1)
//        |create (p1)-[:HAS_TAG]->(t2)
//        |create (p1)-[:HAS_TAG]->(t3)
//        |""".stripMargin)
//
//    val res = db.cypher(s"""
//                 |MATCH (tag:Tag)-[:HAS_TYPE|IS_SUBCLASS_OF*0..]->(baseTagClass:TagClass)
//                 |WHERE tag.name = 'A' OR baseTagClass.name = 'B'
//                 |WITH collect(tag.id) as tags
//                 |MATCH (:Person {id: 1 })-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(:Post)-[:HAS_TAG]->(tag:Tag)
//                 |WHERE tag.id in tags
//                 |RETURN
//                 |    friend.id AS personId,
//                 |    friend.firstName AS personFirstName,
//                 |    friend.lastName AS personLastName,
//                 |    collect(DISTINCT tag.name) AS tagNames,
//                 |    count(DISTINCT comment) AS replyCount
//                 |ORDER BY
//                 |    replyCount DESC,
//                 |    toInteger(personId) ASC
//                 |LIMIT 20
//                 |""".stripMargin).records().next()
//    Assert.assertEquals(2L, res("personId").value)
//    Assert.assertEquals(LynxNull, res("personFirstName"))
//    Assert.assertEquals(LynxNull, res("personLastName"))
//    Assert.assertEquals(List(LynxString("A"), LynxString("T3")), res("tagNames").value)
//    Assert.assertEquals(1L, res("replyCount").value)
//  }
}
