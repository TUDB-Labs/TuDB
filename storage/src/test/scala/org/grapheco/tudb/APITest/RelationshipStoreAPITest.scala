package org.grapheco.tudb.APITest

import org.apache.commons.io.FileUtils
import org.grapheco.tudb.APITest.RelationshipStoreAPITest.{nodeStoreAPI, relationshipStoreAPI}
import org.grapheco.tudb.TuInstanceContext
import org.grapheco.tudb.serializer.{NodeSerializer, RelationshipSerializer}
import org.grapheco.tudb.store.meta.DBNameMap
import org.grapheco.tudb.store.meta.TypeManager.TypeId
import org.grapheco.tudb.store.node.{NodeStoreAPI, StoredNodeWithProperty}
import org.grapheco.tudb.store.relationship.{RelationshipStoreAPI, StoredRelationshipWithProperty}
import org.grapheco.tudb.store.storage.{KeyValueDB, RocksDBStorage}
import org.grapheco.tudb.test.TestUtils
import org.junit._

import java.io.File

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 10:07 上午 2022/2/5
  * @Modified By:
  */
object RelationshipStoreAPITest {
  val outputRoot: String =
    s"${TestUtils.getModuleRootPath}/testOutput/relationshipStoreTest"
  TuInstanceContext.setDataPath(outputRoot)
  val metaDB: KeyValueDB =
    RocksDBStorage.getDB(s"${outputRoot}/${DBNameMap.nodeMetaDB}")
  val nodeStoreAPI = new NodeStoreAPI(
    s"${outputRoot}/${DBNameMap.nodeDB}",
    "default",
    s"${outputRoot}/${DBNameMap.nodeLabelDB}",
    "default",
    metaDB,
    "tudb://index?type=dummy",
    outputRoot
  )
  val relationshipStoreAPI = new RelationshipStoreAPI(
    s"${outputRoot}/${DBNameMap.relationDB}",
    "default",
    s"${outputRoot}/${DBNameMap.inRelationDB}",
    "default",
    s"${outputRoot}/${DBNameMap.outRelationDB}",
    "default",
    s"${outputRoot}/${DBNameMap.relationLabelDB}",
    "default",
    metaDB
  )

  @BeforeClass
  def onStart(): Unit = {
    val file: File = new File(s"${outputRoot}")
    if (file.exists()) FileUtils.deleteDirectory(file)
  }

  @AfterClass
  def onClose(): Unit = {
    nodeStoreAPI.close()
    relationshipStoreAPI.close()
    val file: File = new File(s"${outputRoot}")
    if (file.exists()) FileUtils.deleteDirectory(file)
  }
}

class RelationshipStoreAPITest {
  val labelIds1: Array[Int] = Array(1, 2)
  val labelIds2: Array[Int] = Array(2)
  val props1: Map[Int, Any] =
    Map(1 -> 1L, 2 -> "bluejoe", 3 -> 1979.12, 4 -> "cnic")
  val props2: Map[Int, Any] = Map(2 -> "Airzihao", 3 -> 1994.11, 4 -> "cnic")
  val node1InBytes: Array[Byte] =
    NodeSerializer.encodeNodeWithProperties(1L, labelIds1, props1)
  val node2InBytes: Array[Byte] =
    NodeSerializer.encodeNodeWithProperties(2L, labelIds2, props2)

  val propOfRel1: Map[Int, Any] = Map(100 -> 2017)
  val propOfRel2: Map[Int, Any] = Map(101 -> "2022")
  val type1Id: TypeId = 666
  val type2Id: TypeId = 777
  val rel1InBytes: Array[Byte] =
    RelationshipSerializer.encodeRelationship(12L, 1L, 2L, type1Id, propOfRel1)
  val rel2InBytes: Array[Byte] =
    RelationshipSerializer.encodeRelationship(21L, 2L, 1L, type2Id, propOfRel2)

  @After
  def cleanStore(): Unit = {
    nodeStoreAPI.allNodes().foreach(node => nodeStoreAPI.deleteNode(node.id))
    relationshipStoreAPI
      .allRelations()
      .foreach(relationship => relationshipStoreAPI.deleteRelation(relationship.id))
    Assert.assertFalse(nodeStoreAPI.allNodes().hasNext)
    Assert.assertFalse(relationshipStoreAPI.allRelations().hasNext)
  }

  @Test
  def createAndDeleteTest(): Unit = {
    nodeStoreAPI.addNode(
      new StoredNodeWithProperty(1L, labelIds1, node1InBytes)
    )
    nodeStoreAPI.addNode(
      new StoredNodeWithProperty(2L, labelIds2, node2InBytes)
    )
    Assert.assertFalse(relationshipStoreAPI.allRelations().hasNext)
    relationshipStoreAPI.addRelation(
      new StoredRelationshipWithProperty(12L, 1L, 2L, type1Id, rel1InBytes)
    )
    relationshipStoreAPI.addRelation(
      new StoredRelationshipWithProperty(21L, 2L, 1L, type2Id, rel2InBytes)
    )
    Assert.assertEquals(2, relationshipStoreAPI.allRelations().length)
    relationshipStoreAPI.deleteRelation(12L)
    Assert.assertEquals(1, relationshipStoreAPI.allRelations().length)
    relationshipStoreAPI.deleteRelation(21L)
    Assert.assertEquals(0, relationshipStoreAPI.allRelations().length)
  }

  @Test
  def typeTest(): Unit = {
    nodeStoreAPI.addNode(
      new StoredNodeWithProperty(1L, labelIds1, node1InBytes)
    )
    nodeStoreAPI.addNode(
      new StoredNodeWithProperty(2L, labelIds2, node2InBytes)
    )
    relationshipStoreAPI.addRelation(
      new StoredRelationshipWithProperty(12L, 1L, 2L, type1Id, rel1InBytes)
    )
    relationshipStoreAPI.addRelation(
      new StoredRelationshipWithProperty(21L, 2L, 1L, type2Id, rel2InBytes)
    )
    relationshipStoreAPI.allRelationTypes()
  }

}
