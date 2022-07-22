package org.grapheco.tudb.ldbctest
import org.apache.commons.io.FileUtils
import org.grapheco.lynx.types.property.{LynxInteger, LynxString}
import org.grapheco.tudb.ldbctest.ShortQueryTest.db
import org.grapheco.tudb.{GraphDatabaseBuilder, TuDBInstanceContext}
import org.grapheco.tudb.test.TestUtils
import org.junit.{After, AfterClass, Assert, Test}

import java.io.File

/**
  *@author:John117
  *@createDate:2022/7/4
  *@description:
  */
object ShortQueryTest {
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

class ShortQueryTest {
  @After
  def clean(): Unit = {
    db.cypher("match (n) detach delete n")
  }
  @Test
  def Q1(): Unit = {
    // passed
    db.cypher("""
        |create (AAA:Person {`id`:1,
        |                firstName: "AAA",
        |                lastName: "BBB",
        |                birthday: "2022-07-03 00:00:00.000",
        |                locationIP: "localhost",
        |                browserUsed: "safari",
        |                gender: "man",
        |                creationDate: "2022-07-03 00:00:00.000"}
        |        )
        |create (beijing:City {`id`: 1,
        |               cityName: "beijing"}
        |       )
        |create (AAA) - [:IS_LOCATED_IN] -> (beijing)
        |""".stripMargin)

    val resp = db.cypher(
      """|MATCH (n:Person {id: 1 })-[:IS_LOCATED_IN]->(p:City)
         |   RETURN
         |       n.firstName AS firstName,
         |       n.lastName AS lastName,
         |       n.birthday AS birthday,
         |       n.locationIP AS locationIP,
         |       n.browserUsed AS browserUsed,
         |       p.id AS cityId,
         |       n.gender AS gender,
         |       n.creationDate AS creationDate""".stripMargin
    )
    val result = resp.records().toList

    Assert.assertEquals(1, result.size)
    Assert.assertEquals("AAA", result.head("firstName").asInstanceOf[LynxString].value)
    Assert.assertEquals("BBB", result.head("lastName").asInstanceOf[LynxString].value)
    Assert.assertEquals(
      "2022-07-03 00:00:00.000",
      result.head("birthday").asInstanceOf[LynxString].value
    )
    Assert.assertEquals("localhost", result.head("locationIP").asInstanceOf[LynxString].value)
    Assert.assertEquals("safari", result.head("browserUsed").asInstanceOf[LynxString].value)
    Assert.assertEquals(1, result.head("cityId").asInstanceOf[LynxInteger].value)
    Assert.assertEquals("man", result.head("gender").asInstanceOf[LynxString].value)
    Assert.assertEquals(
      "2022-07-03 00:00:00.000",
      result.head("creationDate").asInstanceOf[LynxString].value
    )
  }
  @Test
  def Q2(): Unit = {
    db.cypher("""
        |create (AAA:Person {`id`:1,
        |                firstName: "AAA",
        |                lastName: "BBB",
        |                birthday: "2022-07-03 00:00:00.000",
        |                locationIP: "localhost",
        |                browserUsed: "safari",
        |                gender: "man",
        |                creationDate: "2022-07-03 00:00:00.000"}
        |        )
        |create (CCC:Person {`id`:2,
        |                firstName: "CCC",
        |                lastName: "DDD",
        |                birthday: "2022-07-03 00:00:00.000",
        |                locationIP: "localhost",
        |                browserUsed: "safari",
        |                gender: "man",
        |                creationDate: "2022-07-03 00:00:00.000"}
        |        )
        |create (EEE:Person {`id`:3,
        |                firstName: "EEE",
        |                lastName: "FFF",
        |                birthday: "2022-07-03 00:00:00.000",
        |                locationIP: "localhost",
        |                browserUsed: "safari",
        |                gender: "man",
        |                creationDate: "2022-07-03 00:00:00.000"}
        |        )
        |create (GGG:Person {`id`:4,
        |                firstName: "GGG",
        |                lastName: "HHH",
        |                birthday: "2022-07-03 00:00:00.000",
        |                locationIP: "localhost",
        |                browserUsed: "safari",
        |                gender: "man",
        |                creationDate: "2022-07-03 00:00:00.000"}
        |        )
        |create (message1:Message {`id`: 1,
        |               creationDate: "2022-07-03 00:00:00.000",
        |               content: "hello"}
        |       )
        |
        |create (message2:Message {`id`: 2,
        |               creationDate: "2022-07-04 00:00:00.000",
        |               imageFile: "/a/b/d.img"}
        |       )
        |
        |create (message3:Message {`id`: 3,
        |               creationDate: "2022-07-04 00:00:00.000",
        |               imageFile: "/a/b/d.img",
        |               content: "world"}
        |       )
        |
        |create (message1)-[:HAS_CREATOR]->(AAA),
        |       (message2)-[:HAS_CREATOR]->(AAA),
        |       (message3)-[:HAS_CREATOR]->(AAA),
        |       (message1)-[:REPLY_OF]->(:Post {`id`:1})-[:HAS_CREATOR]->(CCC),
        |       (message2)-[:REPLY_OF]->(:Post {`id`:2})-[:HAS_CREATOR]->(EEE),
        |       (message3)-[:REPLY_OF]->(:Post {`id`:3})-[:HAS_CREATOR]->(GGG)
        |""".stripMargin)

    val resp = db
      .cypher(
        """
        |MATCH (:Person {id: 1})<-[:HAS_CREATOR]-(message)
        |   WITH
        |    message,
        |    message.id AS messageId,
        |    message.creationDate AS messageCreationDate
        |   ORDER BY messageCreationDate DESC, messageId ASC
        |   LIMIT 10
        |   MATCH (message)-[:REPLY_OF*0..]->(post:Post),
        |         (post)-[r:HAS_CREATOR]->(person)
        |   RETURN
        |    messageId,
        |    coalesce(message.imageFile,message.content) AS messageContent,
        |    messageCreationDate,
        |    post.id AS postId,
        |    person.id AS personId,
        |    person.firstName AS personFirstName,
        |    person.lastName AS personLastName
        |   ORDER BY messageCreationDate DESC, messageId ASC
        |""".stripMargin
      )
      .records()
      .toList

    Assert.assertEquals(3, resp.size)

    val result1 = resp(0)
    val result2 = resp(1)
    val result3 = resp(2)

    Assert.assertEquals(2, result1("messageId").asInstanceOf[LynxInteger].value)
    Assert.assertEquals(2, result1("postId").asInstanceOf[LynxInteger].value)
    Assert.assertEquals(3, result1("personId").asInstanceOf[LynxInteger].value)
    Assert.assertEquals(3, result2("messageId").asInstanceOf[LynxInteger].value)
    Assert.assertEquals(3, result2("postId").asInstanceOf[LynxInteger].value)
    Assert.assertEquals(4, result2("personId").asInstanceOf[LynxInteger].value)
    Assert.assertEquals(1, result3("messageId").asInstanceOf[LynxInteger].value)
    Assert.assertEquals(1, result3("postId").asInstanceOf[LynxInteger].value)
    Assert.assertEquals(2, result3("personId").asInstanceOf[LynxInteger].value)
  }
}
