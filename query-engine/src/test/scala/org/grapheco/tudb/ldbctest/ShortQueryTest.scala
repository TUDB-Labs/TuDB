// Copyright 2022 The TuDB Authors. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.grapheco.tudb.ldbctest
import org.apache.commons.io.FileUtils
import org.grapheco.lynx.types.property.{LynxInteger, LynxString}
import org.grapheco.tudb.ldbctest.ShortQueryTest.db
import org.grapheco.tudb.{GraphDatabaseBuilder, TuDBInstanceContext}
import org.grapheco.tudb.test.TestUtils
import org.junit.{After, AfterClass, Assert, Test}

import java.io.File

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
//  @Test
//  def Q2(): Unit = {
//    db.cypher("""
//        |create (AAA:Person {`id`:1,
//        |                firstName: "AAA",
//        |                lastName: "BBB",
//        |                birthday: "2022-07-03 00:00:00.000",
//        |                locationIP: "localhost",
//        |                browserUsed: "safari",
//        |                gender: "man",
//        |                creationDate: "2022-07-03 00:00:00.000"}
//        |        )
//        |create (CCC:Person {`id`:2,
//        |                firstName: "CCC",
//        |                lastName: "DDD",
//        |                birthday: "2022-07-03 00:00:00.000",
//        |                locationIP: "localhost",
//        |                browserUsed: "safari",
//        |                gender: "man",
//        |                creationDate: "2022-07-03 00:00:00.000"}
//        |        )
//        |create (EEE:Person {`id`:3,
//        |                firstName: "EEE",
//        |                lastName: "FFF",
//        |                birthday: "2022-07-03 00:00:00.000",
//        |                locationIP: "localhost",
//        |                browserUsed: "safari",
//        |                gender: "man",
//        |                creationDate: "2022-07-03 00:00:00.000"}
//        |        )
//        |create (GGG:Person {`id`:4,
//        |                firstName: "GGG",
//        |                lastName: "HHH",
//        |                birthday: "2022-07-03 00:00:00.000",
//        |                locationIP: "localhost",
//        |                browserUsed: "safari",
//        |                gender: "man",
//        |                creationDate: "2022-07-03 00:00:00.000"}
//        |        )
//        |create (message1:Message {`id`: 1,
//        |               creationDate: "2022-07-03 00:00:00.000",
//        |               content: "hello"}
//        |       )
//        |
//        |create (message2:Message {`id`: 2,
//        |               creationDate: "2022-07-04 00:00:00.000",
//        |               imageFile: "/a/b/d.img"}
//        |       )
//        |
//        |create (message3:Message {`id`: 3,
//        |               creationDate: "2022-07-04 00:00:00.000",
//        |               imageFile: "/a/b/d.img",
//        |               content: "world"}
//        |       )
//        |
//        |create (message1)-[:HAS_CREATOR]->(AAA),
//        |       (message2)-[:HAS_CREATOR]->(AAA),
//        |       (message3)-[:HAS_CREATOR]->(AAA),
//        |       (message1)-[:REPLY_OF]->(:Post {`id`:1})-[:HAS_CREATOR]->(CCC),
//        |       (message2)-[:REPLY_OF]->(:Post {`id`:2})-[:HAS_CREATOR]->(EEE),
//        |       (message3)-[:REPLY_OF]->(:Post {`id`:3})-[:HAS_CREATOR]->(GGG)
//        |""".stripMargin)
//
//    val resp = db
//      .cypher(
//        """
//        |MATCH (:Person {id: 1})<-[:HAS_CREATOR]-(message)
//        |   WITH
//        |    message,
//        |    message.id AS messageId,
//        |    message.creationDate AS messageCreationDate
//        |   ORDER BY messageCreationDate DESC, messageId ASC
//        |   LIMIT 10
//        |   MATCH (message)-[:REPLY_OF*0..]->(post:Post),
//        |         (post)-[r:HAS_CREATOR]->(person)
//        |   RETURN
//        |    messageId,
//        |    coalesce(message.imageFile,message.content) AS messageContent,
//        |    messageCreationDate,
//        |    post.id AS postId,
//        |    person.id AS personId,
//        |    person.firstName AS personFirstName,
//        |    person.lastName AS personLastName
//        |   ORDER BY messageCreationDate DESC, messageId ASC
//        |""".stripMargin
//      )
//      .records()
//      .toList
//
//    Assert.assertEquals(3, resp.size)
//
//    val result1 = resp(0)
//    val result2 = resp(1)
//    val result3 = resp(2)
//
//    Assert.assertEquals(2, result1("messageId").asInstanceOf[LynxInteger].value)
//    Assert.assertEquals(2, result1("postId").asInstanceOf[LynxInteger].value)
//    Assert.assertEquals(3, result1("personId").asInstanceOf[LynxInteger].value)
//    Assert.assertEquals(3, result2("messageId").asInstanceOf[LynxInteger].value)
//    Assert.assertEquals(3, result2("postId").asInstanceOf[LynxInteger].value)
//    Assert.assertEquals(4, result2("personId").asInstanceOf[LynxInteger].value)
//    Assert.assertEquals(1, result3("messageId").asInstanceOf[LynxInteger].value)
//    Assert.assertEquals(1, result3("postId").asInstanceOf[LynxInteger].value)
//    Assert.assertEquals(2, result3("personId").asInstanceOf[LynxInteger].value)
//  }
  @Test
  def Q3(): Unit = {
    db.cypher(
      """
        |create (AAA:Person {`id`:"1",
        |                firstName: "AAA",
        |                lastName: "BBB",
        |                birthday: "2022-07-03 00:00:00.000",
        |                locationIP: "localhost",
        |                browserUsed: "safari",
        |                gender: "man",
        |                creationDate: "2022-07-03 00:00:00.000"}
        |        )
        |
        |create (BBB:Person {`id`:"22",
        |                firstName: "BBB",
        |                lastName: "CCC",
        |                birthday: "2022-07-03 00:00:00.000",
        |                locationIP: "localhost",
        |                browserUsed: "safari",
        |                gender: "man",
        |                creationDate: "2022-07-04 00:00:00.000"}
        |        )
        |
        |create (CCC:Person {`id`:"23",
        |                firstName: "CCC",
        |                lastName: "DDD",
        |                birthday: "2022-07-03 00:00:00.000",
        |                locationIP: "localhost",
        |                browserUsed: "safari",
        |                gender: "man",
        |                creationDate: "2022-07-05 00:00:00.000"}
        |        )
        |create (DDD:Person {`id`:"24",
        |                firstName: "DDD",
        |                lastName: "EEE",
        |                birthday: "2022-07-03 00:00:00.000",
        |                locationIP: "localhost",
        |                browserUsed: "safari",
        |                gender: "man",
        |                creationDate: "2022-07-05 00:00:00.000"}
        |        )
        |
        |create (AAA)-[:KNOWS {creationDate: "2022-07-31"}]->(BBB)
        |create (AAA)-[:KNOWS {creationDate: "2022-07-31"}]->(CCC)
        |create (AAA)-[:KNOWS {creationDate: "2022-06-21"}]->(DDD)
        |""".stripMargin
    )

    val resp = db
      .cypher(
        """
        |MATCH (n:Person {id: "1" })-[r:KNOWS]-(friend)
        |   RETURN
        |       friend.id AS personId,
        |       friend.firstName AS firstName,
        |       friend.lastName AS lastName,
        |       r.creationDate AS friendshipCreationDate
        |   ORDER BY
        |       friendshipCreationDate DESC,
        |       toInteger(personId) ASC
        |""".stripMargin
      )
      .records()
      .toList
    Assert.assertEquals(3, resp.size)

    val result1 = resp(0)
    val result2 = resp(1)
    val result3 = resp(2)

    Assert.assertEquals("22", result1("personId").asInstanceOf[LynxString].value)
    Assert.assertEquals(
      "2022-07-31",
      result1("friendshipCreationDate").asInstanceOf[LynxString].value
    )
    Assert.assertEquals("BBB", result1("firstName").asInstanceOf[LynxString].value)
    Assert.assertEquals("23", result2("personId").asInstanceOf[LynxString].value)
    Assert.assertEquals(
      "2022-07-31",
      result2("friendshipCreationDate").asInstanceOf[LynxString].value
    )
    Assert.assertEquals("CCC", result2("firstName").asInstanceOf[LynxString].value)
    Assert.assertEquals("24", result3("personId").asInstanceOf[LynxString].value)
    Assert.assertEquals(
      "2022-06-21",
      result3("friendshipCreationDate").asInstanceOf[LynxString].value
    )
    Assert.assertEquals("DDD", result3("firstName").asInstanceOf[LynxString].value)
  }
  @Test
  def Q4(): Unit = {
    db.cypher(
      """
        |create (:Message {id: 1,
        |                  creationDate: "2022-07-04 00:00:00.000",
        |                  imageFile: "/a/b/d.img"
        |                  }
        |       )
        |
        |create (:Message {id: 2,
        |                  creationDate: "2022-07-04 00:00:00.000",
        |                  imageFile: "/a/b/c.img",
        |                  content: "hello"}
        |       )
        |""".stripMargin
    )

    val resp = db.cypher(
      """
        |MATCH (m:Message {id: 1})
        |   RETURN
        |       m.creationDate as messageCreationDate,
        |       coalesce(m.content, m.imageFile) as messageContent
        |""".stripMargin
    )

    val result = resp.records().next()

    Assert.assertEquals(
      "2022-07-04 00:00:00.000",
      result("messageCreationDate").asInstanceOf[LynxString].value
    )
    Assert.assertEquals("/a/b/d.img", result("messageContent").asInstanceOf[LynxString].value)
  }

  @Test
  def Q5(): Unit = {
    db.cypher(
      """
        |create (:Message {`id`:1,creationDate:"2022-07-06",imageFile:"a/b/c.img",content:"hello"})
        |       - [:HAS_CREATOR] -> (:Person {`id`:1,firstName:"AAA",lastName:"BBB"})
        |""".stripMargin
    )

    val resp = db.cypher(
      """
        |MATCH (m:Message {id:1 })-[:HAS_CREATOR]->(p:Person)
        |   RETURN
        |       p.id AS personId,
        |       p.firstName AS firstName,
        |       p.lastName AS lastName
        |""".stripMargin
    )

    val result = resp.records().next()

    Assert.assertEquals(1, result("personId").asInstanceOf[LynxInteger].value)
    Assert.assertEquals("AAA", result("firstName").asInstanceOf[LynxString].value)
    Assert.assertEquals("BBB", result("lastName").asInstanceOf[LynxString].value)
  }

  @Test
  def Q6(): Unit = {
    db.cypher(
      """
        |create (m:Message {`id`:1}),
        |       (p:Post {`id`:1}),
        |       (f:Forum {`id`:1,title:"scala forum"}),
        |       (pe:Person {`id`:1,firstName:"AAA",lastName:"BBB"})
        |
        |create (m)-[:REPLY_OF]->(p)<-[:CONTAINER_OF]-(f)-[:HAS_MODERATOR]->(pe)
        |""".stripMargin
    )

    val resp = db.cypher(
      """
        |MATCH (m:Message {id: 1 })-[:REPLY_OF*0..]->(p:Post)<-[:CONTAINER_OF]-(f:Forum)-[:HAS_MODERATOR]->(mod:Person)
        |   RETURN
        |       f.id AS forumId,
        |       f.title AS forumTitle,
        |       mod.id AS moderatorId,
        |       mod.firstName AS moderatorFirstName,
        |       mod.lastName AS moderatorLastName
        |""".stripMargin
    )

    val result = resp.records().next()

    Assert.assertEquals(1, result("forumId").asInstanceOf[LynxInteger].value)
    Assert.assertEquals("scala forum", result("forumTitle").asInstanceOf[LynxString].value)
    Assert.assertEquals(1, result("moderatorId").asInstanceOf[LynxInteger].value)
    Assert.assertEquals("AAA", result("moderatorFirstName").asInstanceOf[LynxString].value)
    Assert.assertEquals("BBB", result("moderatorLastName").asInstanceOf[LynxString].value)
  }
}
