import GraphAPIClientTest.testGraphAPIConnectionPort
import org.apache.commons.io.FileUtils
import org.grapheco.tudb.{GraphAPIServer, TuDBInstanceContext, TuDBServerContext}
import org.grapheco.tudb.client.GraphAPIClient
import org.grapheco.tudb.core.Core
import org.grapheco.tudb.test.TestUtils
import org.junit.{After, AfterClass, Assert, Before, BeforeClass, Test}

import java.io.File

object GraphAPIClientTest {
  val testGraphAPIConnectionPort = 3030
  val dbPath: String = s"${TestUtils.getModuleRootPath}/testSpace/testBaseGraphAPI"
  TuDBInstanceContext.setDataPath(dbPath)

  val serverContext = new TuDBServerContext()
  serverContext.setDataPath(dbPath)
  serverContext.setPort(testGraphAPIConnectionPort)
  serverContext.setIndexUri("tudb://index?type=dummy")
  val server: GraphAPIServer = new GraphAPIServer(serverContext)
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

class GraphAPIClientTest {
  var client: GraphAPIClient = null
  @Before
  def setUpClient(): Unit = {
    client = new GraphAPIClient("127.0.0.1", testGraphAPIConnectionPort)
  }

  @After
  def shutDownClient(): Unit = {
    if (client != null) {
      client.shutdown()
    }
  }

  @Test
  def testCreateNode(): Unit = {
    val node = Core.Node.newBuilder().setName("n1").build()
    val createdNode = client.createNode(node)
    Assert.assertEquals(node.getName, createdNode.getName)
  }

  @Test
  def testGetNode(): Unit = {
    val node = Core.Node.newBuilder().setName("n1").build()
    client.createNode(node)
    val obtainedNode = client.getNode("n1")
    Assert.assertEquals(1, obtainedNode.getNodeId)
  }
//
//  @Test
//  def testDeleteNode(): Unit = {
//    client.deleteNode("n1")
//    Assert.assertEquals(null, client.getNode("n1"))
//  }
//
//  @Test
//  def testListNodes(): Unit = {
//    client.createNode(Core.Node.newBuilder().setName("n1").build())
//    client.createNode(Core.Node.newBuilder().setName("n2").build())
//    val nodes = client.listNodes()
//    Assert.assertEquals(2, nodes.length)
//  }
}
