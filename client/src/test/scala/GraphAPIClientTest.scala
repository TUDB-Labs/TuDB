import GraphAPIClientTest.testGraphAPIConnectionPort
import org.apache.commons.io.FileUtils
import org.grapheco.tudb.{GraphAPIServer, NodeService, RelationshipService, TuDBInstanceContext, TuDBServerContext}
import org.grapheco.tudb.client.GraphAPIClient
import org.grapheco.tudb.core.Core
import org.grapheco.tudb.serializer.{NodeSerializer, RelationshipSerializer}
import org.grapheco.tudb.store.meta.TypeManager.TypeId
import org.grapheco.tudb.store.node.StoredNodeWithProperty
import org.grapheco.tudb.store.relationship.StoredRelationshipWithProperty
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
  val labelIds1: Array[Int] = Array(1, 2)
  val props1: Map[Int, Any] =
    Map(1 -> 1L, 2 -> "bluejoe", 3 -> 1979.12, 4 -> "cnic")
  val node1InBytes: Array[Byte] =
    NodeSerializer.encodeNodeWithProperties(1L, labelIds1, props1)
  val storedNode1 = new StoredNodeWithProperty(1L, labelIds1, node1InBytes)

  val labelIds2: Array[Int] = Array(1, 2)
  val props2: Map[Int, Any] =
    Map(1 -> "ok", 2 -> "bad", 3 -> 1.0, 4 -> "good")
  val node2InBytes: Array[Byte] =
    NodeSerializer.encodeNodeWithProperties(2L, labelIds2, props2)
  val storedNode2 = new StoredNodeWithProperty(2L, labelIds2, node2InBytes)

  val propOfRel1: Map[Int, Any] = Map(1 -> 2017)
  val propOfRel2: Map[Int, Any] = Map(2 -> "2022")
  val type1Id: TypeId = 666
  val type2Id: TypeId = 777
  val rel1InBytes: Array[Byte] =
    RelationshipSerializer.encodeRelationship(12L, 1L, 2L, type1Id, propOfRel1)
  val rel2InBytes: Array[Byte] =
    RelationshipSerializer.encodeRelationship(21L, 2L, 1L, type2Id, propOfRel2)
  val storedRelationship1 = new StoredRelationshipWithProperty(12L, 1L, 2L, type1Id, rel1InBytes)
  val storedRelationship2 = new StoredRelationshipWithProperty(21L, 2L, 1L, type2Id, rel2InBytes)

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
    val node: Core.Node = NodeService.ConvertToGrpcNode(storedNode1)
    val createdNode = client.createNode(node)
    Assert.assertEquals(1, createdNode.getNodeId)
    Assert.assertEquals("1", createdNode.getProperties(0).getValue)
    Assert.assertEquals("bluejoe", createdNode.getProperties(1).getValue)
    Assert.assertEquals("1979.12", createdNode.getProperties(2).getValue)
    Assert.assertEquals("cnic", createdNode.getProperties(3).getValue)
    Assert.assertEquals(1, createdNode.getLabelIds(0))
    Assert.assertEquals(2, createdNode.getLabelIds(1))

    val obtainedNode = client.getNode(1)
    Assert.assertEquals(1, obtainedNode.getNodeId)
    Assert.assertEquals("1", obtainedNode.getProperties(0).getValue)
    Assert.assertEquals("bluejoe", obtainedNode.getProperties(1).getValue)
    Assert.assertEquals("1979.12", obtainedNode.getProperties(2).getValue)
    Assert.assertEquals("cnic", obtainedNode.getProperties(3).getValue)
    Assert.assertEquals(1, obtainedNode.getLabelIds(0))
    Assert.assertEquals(2, obtainedNode.getLabelIds(1))

    // TODO: Test case for non-existing node (need to implement error handling).
  }

  @Test
  def testListNodes(): Unit = {
    client.createNode(NodeService.ConvertToGrpcNode(storedNode1))

    val nodes = client.listNodes()
    Assert.assertEquals(1, nodes.length)
    val obtainedNode = nodes.head
    Assert.assertEquals(1, obtainedNode.getNodeId)
    Assert.assertEquals("1", obtainedNode.getProperties(0).getValue)
    Assert.assertEquals("bluejoe", obtainedNode.getProperties(1).getValue)
    Assert.assertEquals("1979.12", obtainedNode.getProperties(2).getValue)
    Assert.assertEquals("cnic", obtainedNode.getProperties(3).getValue)

    client.createNode(NodeService.ConvertToGrpcNode(storedNode2))
    Assert.assertEquals(2, client.listNodes().length)
  }

  @Test
  def testDeleteNode(): Unit = {
    client.deleteNode(1)
    client.deleteNode(2)
    Assert.assertEquals(0, client.listNodes().length)

    client.createNode(NodeService.ConvertToGrpcNode(storedNode1))
    Assert.assertEquals(1, client.listNodes().length)
    // TODO: Delete non-existing node (need to implement error handling).
//    client.deleteNode(2)
  }

  @Test
  def testCreateAndGetRelationship(): Unit = {
    client.createNode(NodeService.ConvertToGrpcNode(storedNode1))
    client.createNode(NodeService.ConvertToGrpcNode(storedNode2))
    val relationship1: Core.Relationship =
      RelationshipService.ConvertToGrpcRelationship(storedRelationship1)
    val createdRelationship = client.createRelationship(relationship1)
    Assert.assertEquals(12L, createdRelationship.getRelationshipId)
    Assert.assertEquals(666, createdRelationship.getRelationType)
    Assert.assertEquals("2017", createdRelationship.getProperties(0).getValue)

    val obtainedRelationship = client.getRelationship(12L)
    Assert.assertEquals(12L, obtainedRelationship.getRelationshipId)
    Assert.assertEquals(666, createdRelationship.getRelationType)
    Assert.assertEquals("2017", obtainedRelationship.getProperties(0).getValue)
  }

  @Test
  def testListRelationships(): Unit = {
    client.createNode(NodeService.ConvertToGrpcNode(storedNode1))
    client.createNode(NodeService.ConvertToGrpcNode(storedNode2))
    val relationship1: Core.Relationship =
      RelationshipService.ConvertToGrpcRelationship(storedRelationship1)
    client.createRelationship(relationship1)
    val relationships = client.listRelationships()
    Assert.assertEquals(1, relationships.length)
    val obtainedRelationship = relationships.head
    Assert.assertEquals(12L, obtainedRelationship.getRelationshipId)
    Assert.assertEquals(666, obtainedRelationship.getRelationType)
    Assert.assertEquals("2017", obtainedRelationship.getProperties(0).getValue)

    val relationship2: Core.Relationship =
      RelationshipService.ConvertToGrpcRelationship(storedRelationship2)
    client.createRelationship(relationship2)
    Assert.assertEquals(2, client.listRelationships().length)
  }

  @Test
  def testDeleteRelationship(): Unit = {
    val relationship1: Core.Relationship =
      RelationshipService.ConvertToGrpcRelationship(storedRelationship1)
    client.createRelationship(relationship1)
    val relationship2: Core.Relationship =
      RelationshipService.ConvertToGrpcRelationship(storedRelationship2)
    client.createRelationship(relationship2)
    client.deleteRelationship(12)
    client.deleteRelationship(21)
    Assert.assertEquals(0, client.listRelationships().length)

    client.createRelationship(relationship1)
    Assert.assertEquals(1, client.listRelationships().length)
  }
}
