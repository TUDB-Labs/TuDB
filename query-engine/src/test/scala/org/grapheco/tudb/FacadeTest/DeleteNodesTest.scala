package org.grapheco.tudb.FacadeTest

import org.apache.commons.io.FileUtils
import org.grapheco.lynx.ConstrainViolatedException
import org.grapheco.lynx.types.structural.{LynxNode, LynxPropertyKey}
import org.grapheco.tudb.{GraphDatabaseBuilder, TuDBInstanceContext}
import org.grapheco.tudb.facade.GraphFacade
import org.grapheco.tudb.test.TestUtils
import org.junit.{After, Assert, Before, Test}

import java.io.File

/**
  *@description:
  */
class DeleteNodesTest {
  val outputPath: String = s"${TestUtils.getModuleRootPath}/facadeTest"
  var db: GraphFacade = _

  @Before
  def init(): Unit = {
    val file = new File(outputPath)
    if (file.exists()) FileUtils.deleteDirectory(file)
    TuDBInstanceContext.setDataPath(outputPath)
    db = GraphDatabaseBuilder.newEmbeddedDatabase(
      TuDBInstanceContext.getDataPath,
      "tudb://index?type=dummy"
    )
  }

  @Test
  def testDeleteNodesWithoutRelationship(): Unit = {
    db.cypher("create (n: Person{name:'A'})")
    db.cypher("create (n: Person{name:'B'})")
    db.cypher("match (n: Person) delete n")
    val res = db.cypher("match (n) return count(n) as count").records().next()("count").value
    Assert.assertEquals(0L, res)

    db.cypher("create (n: Person{name:'A'})")
    db.cypher("create (n: Person{name:'B'})")
    db.cypher("match (n: Person{name:'A'}) delete n")
    db.cypher("match (n: Person{name:'B'}) delete n")

    val res2 = db.cypher("match (n) return count(n) as count").records().next()("count").value
    Assert.assertEquals(0L, res2)
  }

  @Test(expected = classOf[ConstrainViolatedException])
  def testDeleteNodesWithRelationships(): Unit = {
    db.cypher("""
        |create (n:Person{name:'A'})
        |create (m:City{name:'Beijing'})
        |create (n)-[r:LIVE_IN]->(m)
        |""".stripMargin)
    db.cypher("match (n: Person) delete n")
  }
  @Test
  def testDetachDeleteNodesWithRelationships(): Unit = {
    db.cypher("""
                |create (n:Person{name:'A'})
                |create (m:City{name:'Beijing'})
                |create (n)-[r:LIVE_IN]->(m)
                |""".stripMargin)
    db.cypher("match (n: Person) detach delete n")
    val nodeName = db
      .cypher("match (n) return n")
      .records()
      .next()("n")
      .asInstanceOf[LynxNode]
      .property(LynxPropertyKey("name"))
      .get
      .value
    val relCount =
      db.cypher("match (n)-[r]->(m) return count(r) as count ").records().next()("count").value
    Assert.assertEquals("Beijing", nodeName)
    Assert.assertEquals(0L, relCount)
  }

  @After
  def close(): Unit = {
    db.close()
  }
}
