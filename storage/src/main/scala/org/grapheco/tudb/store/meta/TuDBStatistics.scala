package org.grapheco.tudb.store.meta

import org.grapheco.lynx
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxMap
import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey, LynxRelationshipType}
import org.grapheco.tudb.TuStoreContext
import org.grapheco.tudb.serializer.{BaseSerializer, ByteUtils}
import org.grapheco.tudb.store.meta.TuDBStatistics._
import org.grapheco.tudb.store.storage.{KeyValueDB, RocksDBStorage}

import scala.collection.mutable

/** @program: TuDB-Embedded
  * @description:
  * @author: LiamGao
  * @create: 2022-03-17 16:37
  */
object TuDBStatistics {
  val NODESCOUNT: Byte = 1
  val RELATIONSCOUNT: Byte = 2
  val NODECOUNTBYLABEL: Byte = 3
  val RELATIONCOUNTBYTYPE: Byte = 4
  val PROPERTYCOUNTBYINDEX: Byte = 5

  val emptyLong: Array[Byte] = BaseSerializer.encodeLong(0)
}

class TuDBStatistics(path: String, rocksdbCfgPath: String = "default") extends lynx.Statistics {

  val db: KeyValueDB = RocksDBStorage.getDB(
    s"${path}/${DBNameMap.statisticsDB}",
    rocksdbConfigPath = rocksdbCfgPath
  )

  private var _allNodesCount: Long = -1
  private var _allRelationCount: Long = -1
  private var _nodeCountByLabel: mutable.Map[Int, Long] =
    mutable.Map[Int, Long]()
  private var _relationCountByType: mutable.Map[Int, Long] =
    mutable.Map[Int, Long]()
  private var _propertyCountByIndex: mutable.Map[Int, Long] =
    mutable.Map[Int, Long]()

  def getNodeLabelCountMap: Map[Int, Long] = _nodeCountByLabel.toMap
  def getRelationTypeCountMap: Map[Int, Long] = _relationCountByType.toMap

  private def getKey(prefix: Byte, key: Int): Array[Byte] = {
    val res = new Array[Byte](5)
    ByteUtils.setByte(res, 0, prefix)
    ByteUtils.setInt(res, 1, key)
    res
  }

  private def getValue(key: Array[Byte]): Option[Array[Byte]] = {
    Option(db.get(key))
  }

  private def getMap(prefix: Array[Byte]): mutable.Map[Int, Long] = {
    val res = mutable.Map[Int, Long]()
    val iter = db.newIterator()
    iter.seek(prefix)
    while (iter.isValid && iter.key().startsWith(prefix)) {
      res += ByteUtils.getInt(iter.key(), prefix.length) -> BaseSerializer
        .decodeLong(iter.value(), 0)
      iter.next()
    }
    res
  }

  def init(): Unit = {
    _allNodesCount = BaseSerializer.decodeLong(
      getValue(Array(NODESCOUNT)).getOrElse(emptyLong),
      0
    )
    _allRelationCount = BaseSerializer.decodeLong(
      getValue(Array(RELATIONSCOUNT)).getOrElse(emptyLong),
      0
    )
    _nodeCountByLabel.clear()
    _relationCountByType.clear()
    _propertyCountByIndex.clear()
    _nodeCountByLabel = getMap(Array(NODECOUNTBYLABEL))
    _relationCountByType = getMap(Array(RELATIONCOUNTBYTYPE))
    _propertyCountByIndex = getMap(Array(PROPERTYCOUNTBYINDEX))
  }

  def flush(): Unit = {
    db.put(Array(NODESCOUNT), BaseSerializer.encodeLong(_allNodesCount))
    db.put(Array(RELATIONSCOUNT), BaseSerializer.encodeLong(_allRelationCount))
    _nodeCountByLabel.foreach { kv =>
      db.put(getKey(NODECOUNTBYLABEL, kv._1), BaseSerializer.encodeLong(kv._2))
    }
    _relationCountByType.foreach { kv =>
      db.put(
        getKey(RELATIONCOUNTBYTYPE, kv._1),
        BaseSerializer.encodeLong(kv._2)
      )
    }
    _propertyCountByIndex.foreach { kv =>
      db.put(
        getKey(PROPERTYCOUNTBYINDEX, kv._1),
        BaseSerializer.encodeLong(kv._2)
      )
    }
    db.flush()
  }

  def nodeCount: Long = _allNodesCount

  def nodeCount_=(count: Long): Unit = _allNodesCount = count

  def increaseNodeCount(count: Long): Unit = _allNodesCount += count

  def decreaseNodes(count: Long): Unit = _allNodesCount -= count

  def relationCount: Long = _allRelationCount

  def relationCount_=(count: Long): Unit = _allRelationCount = count

  def increaseRelationCount(count: Long): Unit = _allRelationCount += count

  def decreaseRelations(count: Long): Unit = _allRelationCount -= count

  def getNodeLabelCount(labelId: Int): Option[Long] =
    _nodeCountByLabel.get(labelId)

  def addNodeLabelCount(labelId: Int, addBy: Long): Unit = {
    if (_nodeCountByLabel.contains(labelId)) {
      val countBeforeAdd: Long = _nodeCountByLabel.get(labelId).get
      val countAfterAdd: Long = countBeforeAdd + addBy
      _nodeCountByLabel.put(labelId, countAfterAdd)
    }
    _nodeCountByLabel += labelId -> addBy
  }

  def increaseNodeLabelCount(labelId: Int, count: Long): Unit =
    _nodeCountByLabel += labelId -> (_nodeCountByLabel.getOrElse(
      labelId,
      0L
    ) + count)

  def decreaseNodeLabelCount(labelId: Int, count: Long): Unit =
    _nodeCountByLabel += labelId -> (_nodeCountByLabel.getOrElse(
      labelId,
      0L
    ) - count)

  def getRelationTypeCount(typeId: Int): Option[Long] =
    _relationCountByType.get(typeId)

  def addRelationTypeCount(typeId: Int, addBy: Long): Unit = {
    if (_relationCountByType.contains(typeId)) {
      val countBeforeAdd: Long = _relationCountByType.get(typeId).get
      val countAfterAdd: Long = countBeforeAdd + addBy
      _relationCountByType.put(typeId, countAfterAdd)
    } else {
      _relationCountByType += typeId -> addBy
    }
  }

  def increaseRelationTypeCount(typeId: Int, count: Long): Unit =
    _relationCountByType += typeId -> (_relationCountByType.getOrElse(
      typeId,
      0L
    ) + count)

  def decreaseRelationLabelCount(typeId: Int, count: Long): Unit =
    _relationCountByType += typeId -> (_relationCountByType.getOrElse(
      typeId,
      0L
    ) - count)

  def getIndexPropertyCount(indexId: Int): Option[Long] =
    _propertyCountByIndex.get(indexId)

  def setIndexPropertyCount(indexId: Int, count: Long): Unit =
    _propertyCountByIndex += indexId -> count

  def increaseIndexPropertyCount(indexId: Int, count: Long): Unit =
    _propertyCountByIndex += indexId -> (_propertyCountByIndex.getOrElse(
      indexId,
      0L
    ) + count)

  def decreaseIndexPropertyCount(indexId: Int, count: Long): Unit =
    _propertyCountByIndex += indexId -> (_propertyCountByIndex.getOrElse(
      indexId,
      0L
    ) - count)

  def close(): Unit = db.close()

  // Functions below are inherit from Lynx
  override def numNode: Long = nodeCount

  override def numNodeByLabel(labelName: LynxNodeLabel): Long =
    getNodeLabelCount(
      TuStoreContext.getNodeStoreAPI.getLabelId(labelName.value).get
    ).getOrElse(0)

  // todo: Impl this func.
  override def numNodeByProperty(
      labelName: LynxNodeLabel,
      propertyName: LynxPropertyKey,
      value: LynxValue
    ): Long = 0

  override def numRelationship: Long = relationCount

  override def numRelationshipByType(typeName: LynxRelationshipType): Long =
    getRelationTypeCount(
      TuStoreContext.getRelationshipAPI.getRelationTypeId(typeName.value).get
    ).getOrElse(0)

  def getNodeCountByLabel(): LynxMap = {
    LynxMap(_nodeCountByLabel.map {
      case (key, value) =>
        (
          TuStoreContext.getNodeStoreAPI
            .getLabelName(key)
            .getOrElse("unknown"),
          LynxInteger(value)
        )
    }.toMap)
  }

  def getRelationshipCountByType(): LynxMap = {
    LynxMap(_relationCountByType.map {
      case (key, value) =>
        (
          TuStoreContext.getRelationshipAPI
            .getRelationTypeName(key)
            .getOrElse("unknown"),
          LynxInteger(value)
        )
    }.toMap)
  }

}
