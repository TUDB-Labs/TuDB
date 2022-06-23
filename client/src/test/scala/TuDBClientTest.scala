import TuDBClientTest.testConnectionPort
import org.apache.commons.io.FileUtils
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.{LynxInteger, LynxString}
import org.grapheco.lynx.types.structural.{LynxNode, LynxPropertyKey, LynxRelationship}
import org.grapheco.tudb.client.TuDBClient
import org.grapheco.tudb.test.TestUtils
import org.grapheco.tudb.{TuDBServer, TuInstanceContext}
import org.junit.{AfterClass, Assert, BeforeClass, Test}

import java.io.File

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 16:55 2022/4/1
  * @Modified By:
  */

object TuDBClientTest {
  val testConnectionPort = 7600
  val dbPath: String = s"${TestUtils.getModuleRootPath}/testSpace/testBase"
  TuInstanceContext.setDataPath(dbPath)

  val server: TuDBServer = new TuDBServer(testConnectionPort, dbPath,"none")
  @BeforeClass
  def init(): Unit = {
    val dbFile: File = new File(dbPath)
    if (dbFile.exists()) {
      FileUtils.deleteDirectory(new File(dbPath))
    }
    //start the server
    new Thread {
      override def run(): Unit = {
        server.start()
      }
    }.start()
  }

  @AfterClass
  def after(): Unit = {
    server.shutdown()
    val dbFile: File = new File(dbPath)
    if (dbFile.exists()) FileUtils.deleteDirectory(new File(dbPath))
  }
}

class TuDBClientTest {

  @Test
  def test(): Unit = {
    val client: TuDBClient = new TuDBClient("127.0.0.1", testConnectionPort)
    client.query("create (a:DB{name: 'panda1'})-[:use]->(b:RocksDB1);")
    client.query("create (a:DB{name: 'panda2'})-[:use]->(b:RocksDB2);")
    val iter = client.query("Match(n2) Return n2;")
    iter.foreach(println)
    client.shutdown()
  }

  @Test
  def testRelationshipID(): Unit = {
    val client: TuDBClient = new TuDBClient("127.0.0.1", testConnectionPort)
    val stat1 = "Create (n1:START)-[r1:rel]->(n2:Middle)-[r2:rel]->(n3:END);"
    val stat2 =
      "Match p = (n1:START)-[r1:rel]->(n2:Middle)-[r2:rel]->(n3:END) return id(n1), r1, id(n2), r2, id(n3);"
    client.query(stat1)
    val result = client.query(stat2).next()
    val id1 = result.get("id(n1)").get.asInstanceOf[LynxInteger].value
    val id2 = result.get("id(n2)").get.asInstanceOf[LynxInteger].value
    val id3 = result.get("id(n3)").get.asInstanceOf[LynxInteger].value
    val r1 = result.get("r1").get.asInstanceOf[LynxRelationship].value
    val r2 = result.get("r2").get.asInstanceOf[LynxRelationship].value
    Assert.assertFalse(id1 == id2)
    Assert.assertFalse(id2 == id3)
    Assert.assertEquals(id1, r1.startNodeId.toLynxInteger.value)
    Assert.assertEquals(id2, r1.endNodeId.toLynxInteger.value)
    Assert.assertEquals(id2, r2.startNodeId.toLynxInteger.value)
    Assert.assertEquals(id3, r2.endNodeId.toLynxInteger.value)
    client.shutdown()
  }

  @Test
  def testRemoveProp(): Unit = {
    val client: TuDBClient = new TuDBClient("127.0.0.1", testConnectionPort)
    client.query("Create(n:TestRemoveProp{prop1:'prop1', prop2:'prop2'})")
    val prop1 = client
      .query("Match(n:TestRemoveProp) Return n;")
      .next()
      .get("n")
      .get
      .asInstanceOf[LynxNode]
      .property(LynxPropertyKey("prop1"))
      .get
      .asInstanceOf[LynxString]
      .value
    Assert.assertEquals("prop1", prop1)
    val deletedProp1: Option[LynxValue] = client
      .query("Match(n:TestRemoveProp) remove n.prop1 Return n;")
      .next()
      .get("n")
      .get
      .asInstanceOf[LynxNode]
      .property(LynxPropertyKey("prop1"))
    deletedProp1 match {
      case None => Assert.assertTrue(true)
      case _    => Assert.assertTrue(false)
    }
    client.shutdown()
  }

  @Test
  def testEmptyResult(): Unit = {
    val client: TuDBClient = new TuDBClient("127.0.0.1", testConnectionPort)
    val result = client.query("Match(n) WHERE n.prop='On Testing a not existing result' Return n;")
    Assert.assertFalse(result.hasNext)
    client.shutdown()
  }

  @Test
  def testStatistics(): Unit = {
    val client: TuDBClient = new TuDBClient("127.0.0.1", testConnectionPort)
    val statistics = client.getStatistics()
    statistics.head
    client.shutdown()
  }

}
