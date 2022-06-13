package org.grapheco.tudb.importer

import org.grapheco.tudb.serializer.NodeSerializer
import org.rocksdb.{WriteBatch, WriteOptions}

import java.io.File
import java.util.concurrent.ConcurrentHashMap
import scala.collection.convert.ImplicitConversions._
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 10:20 2021/1/15
  * @Modified By:
  */
class SingleNodeFileImporter(
    file: File,
    importCmd: ImportCmd,
    globalArgs: GlobalArgs
) extends SingleFileImporter {

  override val csvFile: File = file
  override val cmd: ImportCmd = importCmd
  override val importerFileReader: ImporterFileReader =
    new ImporterFileReader(file, importCmd.delimeter)
  override val headLine: Array[String] = importerFileReader.getHead.getAsArray
  override val idIndex: Int = {
    val columnID = headLine.indexWhere(item => item.contains(":ID"))
    if (columnID == -1)
      throw new Exception(s"no `:ID` column specify in ${csvFile.getName} file")
    columnID
  }
  override val labelIndex: Int =
    headLine.indexWhere(item => item.contains(":LABEL"))
  override val estLineCount: Long = estLineCount(csvFile)
  override val taskCount: Int = globalArgs.coreNum / 4

  override val propHeadMap: Map[Int, (Int, String)] = {
    headLine.zipWithIndex
      .map(item => {
        if (item._2 == idIndex || item._2 == labelIndex) {
          if (item._1.split(":")(0).length == 0) {
            (-1, (-1, ""))
          } else {
            val pair = item._1.split(":")
            if (pair(0) == "")
              throw new Exception(s"Missed property name in column ${item._2}.")
            val propId = PDBMetaData.getPropId(pair(0))
            val propType = "string"
            (item._2, (propId, propType))
          }
        } else {
          val pair = item._1.split(":")
          val propId = PDBMetaData.getPropId(pair(0))
          val propType = {
            if (pair.length == 2) pair(1).toLowerCase()
            else "string"
          }
          (item._2, (propId, propType))
        }
      })
      .toMap
      .filter(item => item._1 > -1)
  }

  val nodeDB = globalArgs.nodeDB
  val nodeLabelDB = globalArgs.nodeLabelDB

  val innerFileNodeCountByLabel: ConcurrentHashMap[Int, Long] =
    new ConcurrentHashMap[Int, Long]()
  val estNodeCount = globalArgs.estNodeCount
  val NONE_LABEL_ID = -1

  val writeOptions: WriteOptions = new WriteOptions()
  writeOptions.setDisableWAL(true)
  writeOptions.setIgnoreMissingColumnFamilies(true)
  writeOptions.setSync(false)

  override protected def _importTask(taskId: Int): Boolean = {
    val innerTaskNodeCountByLabel: mutable.HashMap[Int, Long] =
      new mutable.HashMap[Int, Long]()
    val serializer = NodeSerializer
    val nodeBatch = new WriteBatch()
    val labelBatch = new WriteBatch()

    while (importerFileReader.notFinished) {
      val batchData = importerFileReader.getLines
      batchData.foreach(line => {
        val lineArr = line.getAsArray
        val node = _wrapNode(lineArr)
        val keys: Array[(Array[Byte], Array[Byte])] =
          _getNodeKeys(node._1, node._2)
        val serializedNodeValue =
          serializer.encodeNodeWithProperties(node._1, node._2, node._3)
        node._2.foreach(labelId =>
          _countMapAdd(innerTaskNodeCountByLabel, labelId, 1L)
        )
        keys.foreach(pair => {
          nodeBatch.put(pair._1, serializedNodeValue)
          labelBatch.put(pair._2, Array.emptyByteArray)
        })

      })
      val f1: Future[Unit] = Future { nodeDB.write(writeOptions, nodeBatch) }
      val f2: Future[Unit] = Future {
        nodeLabelDB.write(writeOptions, labelBatch)
      }
      Await.result(f1, Duration.Inf)
      Await.result(f2, Duration.Inf)

      nodeBatch.clear()
      labelBatch.clear()
      globalArgs.importerStatics.nodeCountAddBy(batchData.length)
      globalArgs.importerStatics.nodePropCountAddBy(
        batchData.length * propHeadMap.size
      )
    }

    innerTaskNodeCountByLabel.foreach(kv =>
      _countMapAdd(innerFileNodeCountByLabel, kv._1, kv._2)
    )
    val f1: Future[Unit] = Future { nodeDB.flush() }
    val f2: Future[Unit] = Future { nodeLabelDB.flush() }
    Await.result(f1, Duration.Inf)
    Await.result(f2, Duration.Inf)
    true
  }

  private def _wrapNode(
      lineArr: Array[String]
  ): (Long, Array[Int], Map[Int, Any]) = {
    val id = lineArr(idIndex).toLong
    val labels: Array[String] = {
      if (labelIndex == -1) {
        new Array[String](0)
      } else {
        lineArr(labelIndex).split(importCmd.arrayDelimeter)
      }
    }
    val labelIds: Array[Int] =
      labels.map(label => PDBMetaData.getLabelId(label))
    val propMap: Map[Int, Any] = _getPropMap(lineArr, propHeadMap)
    (id, labelIds, propMap)
  }

  override protected def _commitInnerFileStatToGlobal(): Boolean = {
    innerFileNodeCountByLabel.foreach(kv =>
      globalArgs.importerStatics.nodeLabelCountAdd(kv._1, kv._2)
    )
    true
  }

  private def _getNodeKeys(
      id: Long,
      labelIds: Array[Int]
  ): Array[(Array[Byte], Array[Byte])] = {
    if (labelIds.isEmpty) {
      val nodeKey = NodeSerializer.encodeNodeKey(id, NONE_LABEL_ID)
      val labelKey = NodeSerializer.encodeNodeLabelKey(id, NONE_LABEL_ID)
      Array((nodeKey, labelKey))
    } else {
      labelIds.map(label => {
        val nodeKey = NodeSerializer.encodeNodeKey(id, label)
        val labelKey = NodeSerializer.encodeNodeLabelKey(id, label)
        (nodeKey, labelKey)
      })
    }
  }
}
