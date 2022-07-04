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
        |create (q1: Message{id:3, creationDate: 2001-01-01 00:00:00.000, content: 'My Content', imageFile: 'iiiii'})
        |create (q2: Message{id:4, creationDate: 2021-01-01 00:00:00.000, imageFile: 'My ImageFile'})
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
                      |    WHERE message.creationDate <= 2011
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
                      |""".stripMargin).records()

//    val data = res.toList
//    Assert.assertEquals(1, data.size)
//    Assert.assertEquals(2L, data.head("personId").value)
  }
}
