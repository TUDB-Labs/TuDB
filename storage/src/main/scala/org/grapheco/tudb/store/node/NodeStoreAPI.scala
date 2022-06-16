package org.grapheco.tudb.store.node

import org.grapheco.lynx.types.LynxValue
import org.grapheco.tudb.serializer.{BaseSerializer, NodeSerializer}
import org.grapheco.tudb.store.meta.TypeManager.NodeId
import org.grapheco.tudb.store.meta.{DBNameMap, IdGenerator, NodeLabelNameStore, PropertyNameStore}
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
                  ) extends NodeStoreSPI {

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

  // add index
  private val propertyIndex = new mutable.HashMap[Any, mutable.HashSet[Long]]()

  val NONE_LABEL_ID: Int = 0

  def this(
            dbPath: String,
            rocksdbCfgPath: String = "default",
            metaDB: KeyValueDB,
            indexMode: String = "hashmap",
            indexUri: String = "",
          ) {
    this(
      s"${dbPath}/${DBNameMap.nodeDB}",
      rocksdbCfgPath,
      s"${dbPath}/${DBNameMap.nodeLabelDB}",
      rocksdbCfgPath,
      metaDB,
      indexUri
    )
  }
  //add all index
  println("start add index")
  indexUri match {
    case "hashmap://mem" =>
      allNodes().foreach { node =>
        node.properties.foreach { property =>
          addPropertyIndex(property._2, node.id)
        }
      }
    case "es" =>
    case _ =>
  }
  println("load index ok")

  /**
   * add node id to index
   *
   * @param value property value
   * @param nodeId
   */
  def addPropertyIndex(value: Any, nodeId: Long): Unit = {
    indexUri match {
      case "hashmap://mem" =>
        val lynxValue = if (value.isInstanceOf[LynxValue]) value.asInstanceOf[LynxValue].value else value
        if (!propertyIndex.contains(lynxValue)) {
          propertyIndex.put(lynxValue, new mutable.HashSet[Long]())
        }
        propertyIndex(lynxValue).add(nodeId.intValue())
      case "es" =>
      case _ =>
    }

  }

  def removePropertyIndex(value: Any, nodeId: Long): Unit = {
    indexUri match {
      case "hashmap://mem" =>
        val lynxValue = if (value.isInstanceOf[LynxValue]) value.asInstanceOf[LynxValue].value else value
        if (propertyIndex.contains(lynxValue)) {
          propertyIndex(lynxValue).remove(nodeId.intValue())
        }
      case "es" =>
      case _ =>
    }
  }

  def removePropertyIndexByNodeId(nodeId: Long): Unit = {
    indexUri match {
      case "hashmap://mem" =>
        getNodeById(nodeId).foreach { node =>
          node.properties.foreach { property =>
            removePropertyIndex(property._2, node.id)
          }
        }
      case "es" =>
      case _ =>
    }
  }

  def getNodeIdByProperty(value: Any): List[Long] = {
    indexUri match {
      case "hashmap://mem" =>
        val lynxValue =
          if (value.isInstanceOf[LynxValue]) value.asInstanceOf[LynxValue].value else value
        propertyIndex.get(lynxValue).map(_.toList).getOrElse(Nil)
      case "es" =>Nil
      case _ =>Nil
    }
  }

  def hasIndex():Boolean={
    indexUri match {
      case "hashmap://mem" =>true
      case "es" =>false //TODO support es
      case _ =>false
    }
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
    nodeLabelStore.get(`nodeId`).map(nodeStore.get(nodeId, _).get)
  }

  override def getNodeById(
                            nodeId: Long,
                            label: Int
                          ): Option[StoredNodeWithProperty] =
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

  override def nodeSetProperty(
                                nodeId: Long,
                                propertyKeyId: Int,
                                propertyValue: Any
                              ): Unit = {
    getNodeById(nodeId)
      .foreach { node => {
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
        addPropertyIndex(propertyValue, nodeId)
      }
      }
  }

  override def nodeRemoveProperty(nodeId: Long, propertyKeyId: Int): Any = {
    getNodeById(nodeId)
      .foreach { node => {
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
        node.properties.get(propertyKeyId).map(propertyValue => removePropertyIndex(propertyValue, nodeId))
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
      addPropertyIndex(property._2, node.id)
    }
  }

  override def addNode(
                        nodeId: NodeId,
                        labelIds: Array[Int],
                        props: Map[Int, Any]
                      ): Unit = {
    addNode(new StoredNodeWithProperty(nodeId, labelIds, props))
  }

  override def allNodes(): Iterator[StoredNodeWithProperty] = nodeStore.all()

  override def nodesCount: Long = nodeLabelStore.getNodesCount

  override def getNodesByLabel(labelId: Int): Iterator[StoredNodeWithProperty] =
    nodeStore.getNodesByLabel(labelId)

  // Fixme: This func is slow, because it needs to finish all the search before return the final iter.
  override def getNodesByLabels(
                                 labelIds: Seq[Int]
                               ): Iterator[StoredNodeWithProperty] =
    labelIds.flatMap(labelId => nodeStore.getNodesByLabel(labelId)).toIterator

  override def getNodeById(
                            nodeId: Long,
                            label: Option[Int]
                          ): Option[StoredNodeWithProperty] =
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
        NodeSerializer.encodeNodeKey(nid, 0),
        NodeSerializer.encodeNodeKey(nid, -1)
      )
      //remove property index
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

  override def serializePropertiesToBytes(
                                           properties: Map[Int, Any]
                                         ): Array[Byte] = {
    NodeSerializer.encodeNodeProperties(properties)
  }

  override def deserializeBytesToProperties(
                                             bytes: Array[Byte]
                                           ): Map[Int, Any] = {
    BaseSerializer.decodePropMap(bytes)
  }
}
