package org.grapheco.tudb.APITest

import org.apache.commons.io.FileUtils
import org.grapheco.tudb.APITest.NodeStoreAPITest.nodeStoreAPI
import org.grapheco.tudb.TuInstanceContext
import org.grapheco.tudb.SerializerTest.SerializerTestBase
import org.grapheco.tudb.serializer.NodeSerializer
import org.grapheco.tudb.store.meta.DBNameMap
import org.grapheco.tudb.store.node.{NodeStoreAPI, StoredNodeWithProperty}
import org.grapheco.tudb.store.storage.{KeyValueDB, RocksDBStorage}
import org.grapheco.tudb.test.TestUtils
import org.junit._
import org.junit.runners.MethodSorters

import java.io.File

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 10:18 上午 2022/2/2
  * @Modified By: LianxinGao
  */

object NodeStoreAPITest {
  val outputRoot: String =
    s"${TestUtils.getModuleRootPath}/testOutput/nodeStoreTest"
  TuInstanceContext.setDataPath(s"$outputRoot")
  val metaDB: KeyValueDB =
    RocksDBStorage.getDB(s"$outputRoot/${DBNameMap.nodeMetaDB}")
  val nodeStoreAPI: NodeStoreAPI = new NodeStoreAPI(
    s"$outputRoot/${DBNameMap.nodeDB}",
    "default",
    s"$outputRoot/${DBNameMap.nodeLabelDB}",
    "default",
    metaDB,
    "hashmap://mem"
  )

  @BeforeClass
  def init(): Unit = {
    val file: File = new File(s"$outputRoot")
    if (file.exists()) FileUtils.deleteDirectory(file)
  }

  @AfterClass
  def close(): Unit = {
    nodeStoreAPI.close()
    metaDB.close()
    val file: File = new File(s"$outputRoot")
    if (file.exists()) FileUtils.deleteDirectory(file)
  }
}

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
class NodeStoreAPITest {

  val labelIds1: Array[Int] = Array(1, 2)
  val labelIds2: Array[Int] = Array(2)
  val props1: Map[Int, Any] =
    Map(1 -> 1L, 2 -> "bluejoe", 3 -> 1979.12, 4 -> "cnic")
  val props2: Map[Int, Any] = Map(2 -> "Airzihao", 3 -> 1994.11, 4 -> "cnic")
  val node1InBytes: Array[Byte] =
    NodeSerializer.encodeNodeWithProperties(1L, labelIds1, props1)
  val node2InBytes: Array[Byte] =
    NodeSerializer.encodeNodeWithProperties(2L, labelIds2, props2)

  @Before
  def cleanNodeStore(): Unit = {
    nodeStoreAPI.allNodes().foreach(node => nodeStoreAPI.deleteNode(node.id))
    Assert.assertFalse(nodeStoreAPI.allNodes().hasNext)
  }

  @Test
  def createAndDeleteTest(): Unit = {
    nodeStoreAPI.addNode(
      new StoredNodeWithProperty(1L, labelIds1, node1InBytes)
    )
    nodeStoreAPI.addNode(
      new StoredNodeWithProperty(2L, labelIds2, node2InBytes)
    )
    Assert.assertEquals(2, nodeStoreAPI.allNodes().length)

    nodeStoreAPI.deleteNode(3L)
    Assert.assertEquals(2, nodeStoreAPI.allNodes().length)
    nodeStoreAPI.deleteNode(1L)
    Assert.assertEquals(1, nodeStoreAPI.allNodes().length)
    nodeStoreAPI.deleteNode(2L)
    Assert.assertFalse(nodeStoreAPI.allNodes().hasNext)
  }

  @Test
  def labelsTest(): Unit = {
    nodeStoreAPI.addNode(
      new StoredNodeWithProperty(1L, labelIds1, node1InBytes)
    )
    nodeStoreAPI.addNode(
      new StoredNodeWithProperty(2L, labelIds2, node2InBytes)
    )

    Assert.assertArrayEquals(
      labelIds1,
      nodeStoreAPI.getNodeById(1L).get.labelIds
    )
    nodeStoreAPI.nodeAddLabel(1L, 3)
    Assert.assertArrayEquals(
      Array(1, 2, 3),
      nodeStoreAPI.getNodeById(1L).get.labelIds
    )
    nodeStoreAPI.nodeRemoveLabel(1L, 1)
    Assert.assertArrayEquals(
      Array(2, 3),
      nodeStoreAPI.getNodeById(1L).get.labelIds
    )
    nodeStoreAPI.nodeRemoveLabel(1L, 2)
    nodeStoreAPI.nodeRemoveLabel(1L, 3)
    Assert.assertArrayEquals(
      Array.emptyIntArray,
      nodeStoreAPI.getNodeById(1L).get.labelIds
    )
  }

  @Test
  def propsTest(): Unit = {
    nodeStoreAPI.addNode(
      new StoredNodeWithProperty(1L, labelIds1, node1InBytes)
    )
    nodeStoreAPI.addNode(
      new StoredNodeWithProperty(2L, labelIds2, node2InBytes)
    )

    Assert.assertTrue(
      props2.sameElements(nodeStoreAPI.getNodeById(2L).get.properties)
    )
    nodeStoreAPI.nodeSetProperty(2L, 4, "cas")
    Assert.assertEquals("cas", nodeStoreAPI.getNodeById(2L).get.properties(4))

    nodeStoreAPI.nodeRemoveProperty(2L, 4)
    Assert.assertEquals(2, nodeStoreAPI.getNodeById(2L).get.properties.size)
  }

  @Test
  def testGetNodeById(): Unit = {
    nodeStoreAPI.addNode(
      new StoredNodeWithProperty(1L, labelIds1, node1InBytes)
    )
    nodeStoreAPI.addNode(
      new StoredNodeWithProperty(2L, labelIds2, node2InBytes)
    )
    val node1 = nodeStoreAPI.getNodeById(1, 1)
    val node2 = nodeStoreAPI.getNodeById(1, 2)
    val node3 = nodeStoreAPI.getNodeById(2)
    Assert.assertEquals(node1.get, node2.get)
    Assert.assertEquals(
      node3.get,
      new StoredNodeWithProperty(2L, labelIds2, node2InBytes)
    )
  }

  @Test
  def testGetLabelIds(): Unit = {
    nodeStoreAPI.addNode(
      new StoredNodeWithProperty(1L, labelIds1, node1InBytes)
    )
    nodeStoreAPI.addNode(
      new StoredNodeWithProperty(2L, labelIds2, node2InBytes)
    )
    val _labelIds1 = nodeStoreAPI.getNodeLabelsById(1)
    val _labelIds2 = nodeStoreAPI.getNodeLabelsById(2)
    Assert.assertTrue(_labelIds1 sameElements labelIds1)
    Assert.assertTrue(_labelIds2 sameElements labelIds2)
  }

  @Test
  def testNodeAddAndRemoveLabel(): Unit = {
    nodeStoreAPI.addNode(
      new StoredNodeWithProperty(1L, labelIds1, node1InBytes)
    )
    nodeStoreAPI.nodeAddLabel(1, 233)
    var _labelIds = nodeStoreAPI.getNodeLabelsById(1)
    Assert.assertTrue(_labelIds sameElements labelIds1 ++ Array(233))

    nodeStoreAPI.nodeRemoveLabel(1, labelIds1.head)
    _labelIds = nodeStoreAPI.getNodeLabelsById(1)
    Assert.assertTrue(_labelIds sameElements labelIds1.tail ++ Array(233))
  }

  @Test
  def testDeleteNodes(): Unit = {
    nodeStoreAPI.addNode(
      new StoredNodeWithProperty(1L, labelIds1, node1InBytes)
    )
    nodeStoreAPI.addNode(
      new StoredNodeWithProperty(2L, labelIds2, node2InBytes)
    )
    Assert.assertEquals(2, nodeStoreAPI.allNodes().size)
    nodeStoreAPI.deleteNodes(Array(1L, 2L).toIterator)
    Assert.assertEquals(0, nodeStoreAPI.allNodes().size)
  }

  @Test
  def testDeleteNodesByLabel(): Unit = {
    nodeStoreAPI.addNode(
      new StoredNodeWithProperty(1L, labelIds1, node1InBytes)
    )
    nodeStoreAPI.addNode(
      new StoredNodeWithProperty(2L, labelIds2, node2InBytes)
    )
    Assert.assertEquals(2, nodeStoreAPI.allNodes().size)
    nodeStoreAPI.deleteNodesByLabel(2)
    Assert.assertEquals(0, nodeStoreAPI.allNodes().size)
  }

  @Test
  def serializeTest(): Unit = {
    val mapValue = Map(1 -> 233, 2 -> "ABC", 3 -> true)
    val labelIds = Array(1, 2, 3)
    val slib = nodeStoreAPI.serializeLabelIdsToBytes(labelIds)
    val spb = nodeStoreAPI.serializePropertiesToBytes(mapValue)
    Assert.assertTrue(
      labelIds sameElements nodeStoreAPI.deserializeBytesToLabelIds(slib)
    )
    val tool = new SerializerTestBase
    tool.propsEqual(mapValue, nodeStoreAPI.deserializeBytesToProperties(spb))
  }

}
