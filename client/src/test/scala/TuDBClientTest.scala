import TuDBClientTest.testConnectionPort
import org.apache.commons.io.FileUtils
import org.grapheco.tudb.client.TuDBClient
import org.grapheco.tudb.network.Query
import org.grapheco.tudb.test.TestUtils
import org.grapheco.tudb.{TuDBInstanceContext, TuDBServer, TuDBServerContext}
import org.junit.{After, AfterClass, Assert, Before, BeforeClass, Test}
import utils.TuQueryResultJsonParseUtil

import java.io.File

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 16:55 2022/4/1
  * @Modified By:
  */
object TuDBClientTest {
  val testConnectionPort = 7600
  val dbPath: String = s"${TestUtils.getModuleRootPath}/testSpace/testBase"
  TuDBInstanceContext.setDataPath(dbPath)

  val serverContext = new TuDBServerContext()
  serverContext.setDataPath(dbPath)
  serverContext.setPort(testConnectionPort)
  serverContext.setIndexUri("tudb://index?type=dummy")
  val server: TuDBServer = new TuDBServer(serverContext)
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
  var client: TuDBClient = null
  @Before
  def setUpClient(): Unit = {
    client = new TuDBClient("127.0.0.1", testConnectionPort)
  }

  @After
  def shutDownlient(): Unit = {
    if (client != null) {
      client.query("match (n) detach delete n")
      client.shutdown()
    }
  }
  @Test
  def testRelationshipID(): Unit = {

    val stat1 = "Create (n1:START)-[r1:rel]->(n2:Middle)-[r2:rel]->(n3:END);"
    client.query(stat1)
    val stat2 =
      "Match p = (n1:START)-[r1:rel]->(n2:Middle)-[r2:rel]->(n3:END) return n1, r1, n2, r2, n3;"
    val queryVO = TuQueryResultJsonParseUtil.parseJsonList(client.query(stat2))

    val id1 = queryVO.get(0).getNodes.get("n1").getId
    val id2 = queryVO.get(0).getNodes.get("n2").getId
    val id3 = queryVO.get(0).getNodes.get("n3").getId
    val r1 = queryVO.get(0).getRelations.get("r1")
    val r2 = queryVO.get(0).getRelations.get("r2")

    Assert.assertFalse(id1 == id2)
    Assert.assertFalse(id2 == id3)
    Assert.assertEquals(id1, r1.getStartId)
    Assert.assertEquals(id2, r1.getEndId)
    Assert.assertEquals(id2, r2.getStartId)
    Assert.assertEquals(id3, r2.getEndId)
  }

  @Test
  def testRemoveProp(): Unit = {

    client.query("Create(n:TestRemoveProp{prop1:'prop1', prop2:'prop2'})")
    val result = TuQueryResultJsonParseUtil.parseJsonList(
      client.query("Match(n:TestRemoveProp) Return n;")
    )
    val prop1 = result.get(0).getNodes.get("n").getProperties.get("prop1")
    Assert.assertEquals("prop1", prop1)

    val deleted = TuQueryResultJsonParseUtil.parseJsonList(
      client.query("Match(n:TestRemoveProp) remove n.prop1 Return n;")
    )
    val deletedProp1 = deleted.get(0).getNodes.get("n").getProperties.get("prop1")
    deletedProp1 match {
      case null => Assert.assertTrue(true)
      case _    => Assert.assertTrue(false)
    }

  }

  @Test
  def testEmptyResult(): Unit = {
    val result = TuQueryResultJsonParseUtil.parseJsonList(
      client.query("Match(n) WHERE n.prop='On Testing a not existing result' Return n;")
    )
    Assert.assertTrue(result.isEmpty)
  }

  @Test
  def testStatistics(): Unit = {

    client.query("create (n:person1)-[r:KNOW]->(m:person2)")
    client.query("create (n:person3)-[r:KNOW]->(m:person2)")
    val statistics: List[Any] = client.getStatistics()
    val nodeStat = statistics(0).asInstanceOf[Map[String, Integer]]
    val relStat = statistics(1).asInstanceOf[Map[String, Integer]]

    Assert.assertEquals(1, nodeStat.get("person1").get)
    Assert.assertEquals(2, nodeStat.get("person2").get)
    Assert.assertEquals(1, nodeStat.get("person3").get)
    Assert.assertEquals(2, relStat.get("KNOW").get)
  }

  @Test
  def testCypherException(): Unit = {
    val request: Query.QueryRequest =
      Query.QueryRequest.newBuilder().setStatement("321321321321").build()
    val response = client.blockingStub.query(request)
    if (response.hasNext) {
      val message = response.next().getMessage
      Assert.assertTrue(message.startsWith("Invalid input"))
    }
  }

}
