// Copyright 2022 The TuDB Authors. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.grapheco.tudb.store.node

import org.grapheco.lynx.types.LynxValue
import org.grapheco.tudb.serializer.{BaseSerializer, NodeSerializer}
import org.grapheco.tudb.store.index.IndexFactory
import org.grapheco.tudb.store.meta.TypeManager.NodeId
import org.grapheco.tudb.store.meta.{ConfigNameMap, DBNameMap, IdGenerator, NodeLabelNameStore, PropertyNameStore}
import org.grapheco.tudb.store.storage.{KeyValueDB, RocksDBStorage}
import org.rocksdb.{WriteBatch, WriteOptions}

import scala.collection.mutable

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 12:14 下午 2022/1/29
  * @Modified By:
  */
// NodeStoreAPI is supplied for the Query Engine
class NodeStoreAPI(
    nodeDBPath: String,
    nodeDBConfigPath: String,
    nodeLabelDBPath: String,
    nodeLabelConfigPath: String,
    metaDB: KeyValueDB,
    indexUri: String,
    dbPath: String)
  extends NodeStoreSPI {

  private val nodeDB =
    RocksDBStorage.getDB(nodeDBPath, rocksdbConfigPath = nodeDBConfigPath)
  private val nodeLabelDB = RocksDBStorage.getDB(
    nodeLabelDBPath,
    rocksdbConfigPath = nodeLabelConfigPath
  )

  // Fixme: Don't disable the WAL by hard code.
  private val writeOptions = new WriteOptions()
  writeOptions.setDisableWAL(true)
  writeOptions.setIgnoreMissingColumnFamilies(true)
  writeOptions.setSync(false)
  private val nodeStore = new NodeStore(nodeDB)
  private val nodeLabelStore = new NodeLabelStore(nodeLabelDB)
  private val nodeLabelName = new NodeLabelNameStore(metaDB)
  private val propertyName = new PropertyNameStore(metaDB)

  private val idGenerator = new IdGenerator(nodeLabelDB, 200)
  // this is the index engine instance
  private val indexImpl = IndexFactory.newIndex(indexUri + "&path=" + dbPath)

  val NONE_LABEL_ID: Int = 0

  def this(
      dbPath: String,
      rocksdbCfgPath: String = "default",
      metaDB: KeyValueDB,
      indexUri: String
    ) {
    this(
      s"${dbPath}/${DBNameMap.nodeDB}",
      rocksdbCfgPath,
      s"${dbPath}/${DBNameMap.nodeLabelDB}",
      rocksdbCfgPath,
      metaDB,
      indexUri,
      dbPath
    )
  }
  //add all index
  logger.info("start add index")
  var addCount = 0
  if (indexImpl.hasIndex() && needRebuildIndex()) {
    //generate index for all node    use memory to speedup
    val cacheHashMap = new mutable.HashMap[String, mutable.HashSet[Long]]()
    allNodes().foreach { node =>
      node.properties.foreach { property =>
        val key = indexImpl.encodeKey(property._1, property._2)
        if (cacheHashMap.contains(key)) {
          cacheHashMap(key).add(node.id)
          if (cacheHashMap(key).size >= 100000) { //batch add index TODO size can be config
            indexImpl.batchAddIndex(key, cacheHashMap(key).toSet)
            cacheHashMap(key).clear()
          }
        } else {
          cacheHashMap(key) = new mutable.HashSet[Long]()
          cacheHashMap(key).add(node.id)
        }
        addCount += 1
      }
    }
    cacheHashMap.foreach { //batch add index
      case (key, value) =>
        indexImpl.batchAddIndex(key, value.toSet)
    }
  }
  metaDB.put(ConfigNameMap.indexNameStorageKey, indexImpl.indexName.getBytes)
  logger.info(f"load index ok,size:${addCount}")

  /**
    *
    * @return true if need rebuild index
    */
  def needRebuildIndex(): Boolean = {
    // check last time use index engine
    val indexType = metaDB.get(ConfigNameMap.indexNameStorageKey)
    if (indexType == null || indexType.length == 0) {
      true
    } else {
      indexImpl.needRebuildIndex(indexType.toString)
    }

  }

  def removePropertyIndexByNodeId(nodeId: Long): Unit = {
    if (indexImpl.hasIndex()) {
      getNodeById(nodeId).foreach { node =>
        node.properties.foreach { property =>
          indexImpl.removeIndex(indexImpl.encodeKey(property._1, property._2), node.id)
        }
      }
    }
  }

  /**
    * @see [[NodeStoreSPI.getNodeIdByProperty()]]
    *  @return bool
    */
  def getNodeIdByProperty(propertyKey: Int, propertyValue: Any): Set[Long] = {
    indexImpl.getIndexByKey(indexImpl.encodeKey(propertyKey, propertyValue))
  }

  /**
    * @see [[NodeStoreSPI.hasIndex()]]
    *  @return bool
    */
  def hasIndex(): Boolean = {
    indexImpl.hasIndex()
  }

  override def allLabels(): Array[String] =
    nodeLabelName.mapString2Int.keys.toArray

  override def allLabelIds(): Array[Int] =
    nodeLabelName.mapInt2String.keys.toArray

  override def getLabelName(labelId: Int): Option[String] =
    nodeLabelName.key(labelId)

  override def getLabelId(labelName: String): Option[Int] =
    nodeLabelName.id(labelName)

  override def getLabelIds(labelNames: Set[String]): Set[Int] =
    nodeLabelName.ids(labelNames)

  override def addLabel(labelName: String): Int =
    nodeLabelName.getOrAddId(labelName)

  override def allPropertyKeys(): Array[String] =
    propertyName.mapString2Int.keys.toArray

  override def allPropertyKeyIds(): Array[Int] =
    propertyName.mapInt2String.keys.toArray

  override def getPropertyKeyName(keyId: Int): Option[String] =
    propertyName.key(keyId)

  override def getPropertyKeyId(keyName: String): Option[Int] =
    propertyName.id(keyName)

  override def addPropertyKey(keyName: String): Int =
    propertyName.getOrAddId(keyName)

  override def getNodeById(nodeId: Long): Option[StoredNodeWithProperty] = {
    nodeLabelStore.get(nodeId).map(nodeStore.get(nodeId, _).get)
  }

  override def getNodeById(nodeId: Long, label: Int): Option[StoredNodeWithProperty] =
    nodeStore.get(nodeId, label)

  override def getNodeLabelsById(nodeId: Long): Array[Int] =
    nodeLabelStore.getAll(nodeId)

  override def hasLabel(nodeId: Long, label: Int): Boolean =
    nodeLabelStore.exist(nodeId, label)

  override def nodeAddLabel(nodeId: Long, labelId: Int): Unit =
    getNodeById(nodeId)
      .foreach { node =>
        if (!node.labelIds.contains(labelId)) {
          val labels = node.labelIds ++ Array(labelId)
          nodeLabelStore.set(nodeId, labelId)
          val nodeInBytes: Array[Byte] = NodeSerializer
            .encodeNodeWithProperties(node.id, labels, node.properties)
          nodeStore.set(
            new StoredNodeWithProperty(node.id, labels, nodeInBytes)
          )
          // if node is nonLabel node, delete it
          if (node.labelIds.isEmpty) {
            nodeLabelStore.delete(nodeId, NONE_LABEL_ID)
            nodeStore.delete(nodeId, NONE_LABEL_ID)
          }
        }
      }

  override def nodeRemoveLabel(nodeId: Long, labelId: Int): Unit =
    nodeStore
      .get(nodeId, labelId)
      .foreach { node =>
        if (node.labelIds.contains(labelId)) {
          val labels = node.labelIds.filter(_ != labelId)
          val nodeInBytes: Array[Byte] = NodeSerializer
            .encodeNodeWithProperties(node.id, labels, node.properties)
          val newNode = new StoredNodeWithProperty(node.id, labels, nodeInBytes)
          // if a node has only one label, add NONE_LABEL_ID after delete it
          if (node.labelIds.length == 1) {
            nodeLabelStore.set(nodeId, NONE_LABEL_ID)
            nodeStore.set(NONE_LABEL_ID, newNode)
          }
          nodeLabelStore.delete(node.id, labelId)
          nodeStore.set(newNode)
          nodeStore.delete(nodeId, labelId)
        }
      }

  override def nodeSetProperty(nodeId: Long, propertyKeyId: Int, propertyValue: Any): Unit = {
    getNodeById(nodeId)
      .foreach { node =>
        {
          val nodeInBytes: Array[Byte] =
            NodeSerializer.encodeNodeWithProperties(
              node.id,
              node.labelIds,
              node.properties ++ Map(propertyKeyId -> propertyValue)
            )
          nodeStore.set(
            new StoredNodeWithProperty(node.id, node.labelIds, nodeInBytes)
          )
          //add node id to index
          indexImpl.addIndex(indexImpl.encodeKey(propertyKeyId, propertyValue), nodeId)
        }
      }
  }

  override def nodeRemoveProperty(nodeId: Long, propertyKeyId: Int): Any = {
    getNodeById(nodeId)
      .foreach { node =>
        {
          val nodeInBytes: Array[Byte] =
            NodeSerializer.encodeNodeWithProperties(
              node.id,
              node.labelIds,
              node.properties - propertyKeyId
            )
          nodeStore.set(
            new StoredNodeWithProperty(node.id, node.labelIds, nodeInBytes)
          )
          //remove node id from index
          node.properties
            .get(propertyKeyId)
            .map(propertyValue =>
              indexImpl.removeIndex(indexImpl.encodeKey(propertyKeyId, propertyValue), nodeId)
            )
        }
      }
  }

  override def addNode(node: StoredNodeWithProperty): Unit = {
    if (node.labelIds != null && node.labelIds.length > 0) {
      nodeStore.set(node)
      nodeLabelStore.set(node.id, node.labelIds)
    } else {
      nodeStore.set(NONE_LABEL_ID, node)
      nodeLabelStore.set(node.id, NONE_LABEL_ID)
    }
    //add node id to index
    node.properties.foreach { property =>
      indexImpl.addIndex(indexImpl.encodeKey(property._1, property._2), node.id)
    }
  }

  override def addNode(nodeId: NodeId, labelIds: Array[Int], props: Map[Int, Any]): Unit = {
    addNode(new StoredNodeWithProperty(nodeId, labelIds, props))
  }

  override def allNodes(): Iterator[StoredNodeWithProperty] = nodeStore.all()

  override def nodesCount: Long = nodeLabelStore.getNodesCount

  override def getNodesByLabel(labelId: Int): Iterator[StoredNodeWithProperty] =
    nodeStore.getNodesByLabel(labelId)

  // Fixme: This func is slow, because it needs to finish all the search before return the final iter.
  override def getNodesByLabels(labelIds: Seq[Int]): Iterator[StoredNodeWithProperty] =
    labelIds.flatMap(labelId => nodeStore.getNodesByLabel(labelId)).toIterator

  override def getNodeById(nodeId: Long, label: Option[Int]): Option[StoredNodeWithProperty] =
    label.map(getNodeById(nodeId, _)).getOrElse(getNodeById(nodeId))

  override def getNodeIdsByLabel(labelId: Int): Iterator[Long] =
    nodeStore.getNodeIdsByLabel(labelId)

  override def deleteNode(nodeId: Long): Unit = {
    removePropertyIndexByNodeId(nodeId)
    nodeLabelStore
      .getAll(nodeId)
      .foreach(labelId => nodeStore.delete(nodeId, labelId))
    nodeLabelStore.delete(nodeId)
  }

  override def deleteNodes(nodeIDs: Iterator[NodeId]): Unit = {
    val nodesWB = new WriteBatch()
    val labelWB = new WriteBatch()
    nodeIDs.foreach(nid => {
      nodeLabelStore
        .getAll(nid)
        .foreach(lid => {
          nodesWB.delete(NodeSerializer.encodeNodeKey(nid, lid))
        })
      labelWB.deleteRange(
        NodeSerializer.encodeNodeLabelKey(nid, 0),
        NodeSerializer.encodeNodeLabelKey(nid, -1)
      )
      removePropertyIndexByNodeId(nid)
    })
    nodeDB.write(writeOptions, nodesWB) //TODO Important! to guarantee atomic
    nodeLabelDB.write(
      writeOptions,
      labelWB
    ) //TODO Important! to guarantee atomic
  }

  // big cost
  override def deleteNodesByLabel(labelId: Int): Unit = {
    nodeStore
      .getNodeIdsByLabel(labelId)
      .foreach { nodeid =>
        nodeLabelStore
          .getAll(nodeid)
          .foreach {
            nodeStore.delete(nodeid, _)
          }
        nodeLabelStore.delete(nodeid)
        removePropertyIndexByNodeId(nodeid)
      }
    nodeStore.deleteByLabel(labelId)
  }

  override def close(): Unit = {
    indexImpl.close()
    nodeDB.close()
    nodeLabelDB.close()
    metaDB.close()
  }

  override def newNodeId(): Long = {
    idGenerator.nextId()
  }

  override def refreshMeta(): Unit = {}

  override def cleanData(): Unit = {}

  override def serializeLabelIdsToBytes(labelIds: Array[Int]): Array[Byte] = {
    BaseSerializer.encodeArray(labelIds)
  }

  override def deserializeBytesToLabelIds(bytes: Array[Byte]): Array[Int] = {
    NodeSerializer.decodeNodeLabelIds(bytes)
  }

  override def serializePropertiesToBytes(properties: Map[Int, Any]): Array[Byte] = {
    NodeSerializer.encodeNodeProperties(properties)
  }

  override def deserializeBytesToProperties(bytes: Array[Byte]): Map[Int, Any] = {
    BaseSerializer.decodePropMap(bytes)
  }
}
