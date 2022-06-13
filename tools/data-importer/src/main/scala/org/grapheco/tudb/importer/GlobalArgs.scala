package org.grapheco.tudb.importer

import org.grapheco.tudb.store.meta.TuDBStatistics
import org.grapheco.tudb.store.storage.KeyValueDB

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.collection.convert.ImplicitConversions._

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 15:07 2021/1/20
  * @Modified By:
  */
case class GlobalArgs(
    coreNum: Int = Runtime.getRuntime().availableProcessors(),
    importerStatics: ImporterStatics,
    estNodeCount: Long,
    estRelCount: Long,
    nodeDB: KeyValueDB,
    nodeLabelDB: KeyValueDB,
    relationDB: KeyValueDB,
    inRelationDB: KeyValueDB,
    outRelationDB: KeyValueDB,
    relationTypeDB: KeyValueDB,
    statistics: TuDBStatistics
)

case class ImporterStatics() {
  private val globalNodeCount: AtomicLong = new AtomicLong(0)
  private val globalRelCount: AtomicLong = new AtomicLong(0)
  private val globalNodePropCount: AtomicLong = new AtomicLong(0)
  private val globalRelPropCount: AtomicLong = new AtomicLong(0)

  private val nodeCountByLabel: ConcurrentHashMap[Int, Long] =
    new ConcurrentHashMap[Int, Long]()
  private val relCountByType: ConcurrentHashMap[Int, Long] =
    new ConcurrentHashMap[Int, Long]()

  def getGlobalNodeCount = globalNodeCount
  def getGlobalRelCount = globalRelCount
  def getGlobalNodePropCount = globalNodePropCount
  def getGlobalRelPropCount = globalRelPropCount

  def getNodeCountByLabel: Map[Int, Long] = nodeCountByLabel.toMap

  def getRelCountByType: Map[Int, Long] = relCountByType.toMap

  def nodeCountAddBy(count: Long): Long = globalNodeCount.addAndGet(count)

  def relCountAddBy(count: Long): Long = globalRelCount.addAndGet(count)

  def nodePropCountAddBy(count: Long): Long =
    globalNodePropCount.addAndGet(count)

  def relPropCountAddBy(count: Long): Long = globalRelPropCount.addAndGet(count)

  def nodeLabelCountAdd(label: Int, addBy: Long): Long = this.synchronized {
    if (nodeCountByLabel.containsKey(label)) {
      val countBeforeAdd = nodeCountByLabel.get(label)
      val countAfterAdd = countBeforeAdd + addBy
      nodeCountByLabel.put(label, countAfterAdd)
      countAfterAdd
    } else {
      nodeCountByLabel.put(label, addBy)
      addBy
    }
  }

  def relTypeCountAdd(typeId: Int, addBy: Long): Long = this.synchronized {
    if (relCountByType.containsKey(typeId)) {
      val countBeforeAdd = relCountByType.get(typeId)
      val countAfterAdd = countBeforeAdd + addBy
      relCountByType.put(typeId, countAfterAdd)
      countAfterAdd
    } else {
      relCountByType.put(typeId, addBy)
      addBy
    }
  }
}
