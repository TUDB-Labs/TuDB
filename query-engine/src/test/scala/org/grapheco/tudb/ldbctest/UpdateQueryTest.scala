package org.grapheco.tudb.ldbctest

import org.apache.commons.io.FileUtils
import org.grapheco.tudb.ldbctest.UpdateQueryTest.db
import org.grapheco.tudb.{GraphDatabaseBuilder, TuDBInstanceContext}
import org.grapheco.tudb.test.TestUtils
import org.junit.{After, AfterClass, Assert, Test}
import org.junit.Test

import java.io.File

/**
  *@author:John117
  *@createDate:2022/7/4
  *@description:
  */
object UpdateQueryTest {
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

class UpdateQueryTest {
  @After
  def clean(): Unit = {
    db.cypher("match (n) detach delete n")
  }

  @Test
  def Q1(): Unit = {}
}
