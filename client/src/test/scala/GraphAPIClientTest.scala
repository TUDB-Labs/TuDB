import GraphAPIClientTest.testGraphAPIConnectionPort
import org.apache.commons.io.FileUtils
import org.grapheco.tudb.{GraphAPIServer, NodeService, TuDBInstanceContext, TuDBServerContext}
import org.grapheco.tudb.client.GraphAPIClient
import org.grapheco.tudb.core.Core
import org.grapheco.tudb.serializer.NodeSerializer
import org.grapheco.tudb.store.node.StoredNodeWithProperty
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
  def testCreateAndGetNode(): Unit = {
    val labelIds1: Array[Int] = Array(1, 2)
    val props1: Map[Int, Any] =
      Map(1 -> 1L, 2 -> "bluejoe", 3 -> 1979.12, 4 -> "cnic")
    val node1InBytes: Array[Byte] =
      NodeSerializer.encodeNodeWithProperties(1L, labelIds1, props1)
    val storedNode = new StoredNodeWithProperty(1L, labelIds1, node1InBytes)
    val node: Core.Node = NodeService.ConvertToGrpcNode(storedNode)
    val createdNode = client.createNode(node)
    Assert.assertEquals(1, createdNode.getNodeId)
    Assert.assertEquals("1", createdNode.getProperties(0).getValue)
    Assert.assertEquals("bluejoe", createdNode.getProperties(1).getValue)
    Assert.assertEquals("1979.12", createdNode.getProperties(2).getValue)
    Assert.assertEquals("cnic", createdNode.getProperties(3).getValue)

    val obtainedNode = client.getNode(1)
    Assert.assertEquals(1, obtainedNode.getNodeId)
    Assert.assertEquals("1", obtainedNode.getProperties(0).getValue)
    Assert.assertEquals("bluejoe", obtainedNode.getProperties(1).getValue)
    Assert.assertEquals("1979.12", obtainedNode.getProperties(2).getValue)
    Assert.assertEquals("cnic", obtainedNode.getProperties(3).getValue)

    // TODO: Test case for non-existing node
  }

//  @Test
//  def testListNodes(): Unit = {
//    val nodes = client.listNodes()
//    Assert.assertEquals(1, nodes.length)
//    val obtainedNode = nodes.head
//    Assert.assertEquals(1, obtainedNode.getNodeId)
//    Assert.assertEquals("1", obtainedNode.getProperties(0).getValue)
//    Assert.assertEquals("bluejoe", obtainedNode.getProperties(1).getValue)
//    Assert.assertEquals("1979.12", obtainedNode.getProperties(2).getValue)
//    Assert.assertEquals("cnic", obtainedNode.getProperties(3).getValue)
//  }

  @Test
  def testDeleteNode(): Unit = {
    client.deleteNode(1)
    // TODO: Check if node still exists
    // TODO: Delete non-existing node
//    client.deleteNode(2)
  }
}
