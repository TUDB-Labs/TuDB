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

package org.grapheco.tudb.importer

import org.grapheco.tudb.serializer.{BaseSerializer, RelationshipSerializer}
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
  * @Date: Created in 21:18 2021/1/15
  * @Modified By:
  */
class SingleRelationFileImporter(file: File, importCmd: ImportCmd, globalArgs: GlobalArgs)
  extends SingleFileImporter {

  override val csvFile: File = file
  override val cmd: ImportCmd = importCmd
  override val importerFileReader: ImporterFileReader =
    new ImporterFileReader(csvFile, importCmd.delimeter)
  override val headLine: Array[String] = importerFileReader.getHead.getAsArray
  override val idIndex: Int = {
    val columnId: Int = headLine.indexWhere(item => item.contains("REL_ID"))
    if (columnId == -1) throw new Exception("No :REL_ID column.")
    columnId
  }
  override val labelIndex: Int = {
    val columnId: Int = headLine.indexWhere(item => item.contains(":TYPE"))
    if (columnId == -1) throw new Exception("No :TYPE column.")
    columnId
  }
  override val estLineCount: Long = estLineCount(csvFile)
  override val taskCount: Int = globalArgs.coreNum / 4

  val fromIdIndex: Int = headLine.indexWhere(item => item.contains(":START_ID"))
  val toIdIndex: Int = headLine.indexWhere(item => item.contains(":END_ID"))

  override val propHeadMap: Map[Int, (Int, String)] = {
    headLine.zipWithIndex
      .map(item => {
        if (item._2 == idIndex || item._2 == labelIndex || item._2 == fromIdIndex || item._2 == toIdIndex) {
          if (item._1.split(":")(0).length == 0 || item._1
                .split(":")(0) == "REL_ID") {
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

  val relationDB = globalArgs.relationDB
  val inRelationDB = globalArgs.inRelationDB
  val outRelationDB = globalArgs.outRelationDB
  val relationTypeDB = globalArgs.relationTypeDB

  val writeOptions: WriteOptions = new WriteOptions()
  writeOptions.setDisableWAL(true)
  writeOptions.setIgnoreMissingColumnFamilies(true)
  writeOptions.setSync(false)

  val innerFileRelCountByType: ConcurrentHashMap[Int, Long] =
    new ConcurrentHashMap[Int, Long]()
  val estEdgeCount: Long = estLineCount

  override protected def _importTask(taskId: Int): Boolean = {
    val innerTaskRelCountByType: mutable.HashMap[Int, Long] =
      new mutable.HashMap[Int, Long]()
    val serializer = RelationshipSerializer

    val inBatch = new WriteBatch()
    val outBatch = new WriteBatch()
    val storeBatch = new WriteBatch()
    val labelBatch = new WriteBatch()

    while (importerFileReader.notFinished) {
      val batchData = importerFileReader.getLines
      if (batchData.nonEmpty) {
        batchData.foreach(line => {
          val lineArr = line.getAsArray
          val relation = _wrapEdge(lineArr)
          val serializedRel = serializer.encodeRelationship(
            relation._1,
            relation._2,
            relation._3,
            relation._4,
            relation._5
          )
          _countMapAdd(innerTaskRelCountByType, relation._4, 1L)
          storeBatch.put(
            RelationshipSerializer.encodeRelationshipKey(relation._1),
            serializedRel
          )
          inBatch.put(
            RelationshipSerializer.encodeDirectedRelationshipKey(
              relation._3,
              relation._4,
              relation._2
            ),
            BaseSerializer.encodeLong(relation._1)
          )
          outBatch.put(
            RelationshipSerializer.encodeDirectedRelationshipKey(
              relation._2,
              relation._4,
              relation._3
            ),
            BaseSerializer.encodeLong(relation._1)
          )
          labelBatch.put(
            RelationshipSerializer
              .encodeRelationshipTypeKey(relation._4, relation._1),
            Array.emptyByteArray
          )

        })
        val f1: Future[Unit] = Future {
          relationDB.write(writeOptions, storeBatch)
        }
        val f2: Future[Unit] = Future {
          inRelationDB.write(writeOptions, inBatch)
        }
        val f3: Future[Unit] = Future {
          outRelationDB.write(writeOptions, outBatch)
        }
        val f4: Future[Unit] = Future {
          relationTypeDB.write(writeOptions, labelBatch)
        }
        Await.result(f1, Duration.Inf)
        Await.result(f2, Duration.Inf)
        Await.result(f3, Duration.Inf)
        Await.result(f4, Duration.Inf)

        storeBatch.clear()
        inBatch.clear()
        outBatch.clear()
        labelBatch.clear()
        globalArgs.importerStatics.relCountAddBy(batchData.length)
        globalArgs.importerStatics.relPropCountAddBy(
          batchData.length * propHeadMap.size
        )
      }
    }

    innerTaskRelCountByType.foreach(kv => _countMapAdd(innerFileRelCountByType, kv._1, kv._2))
    val f1: Future[Unit] = Future { relationDB.flush() }
    val f2: Future[Unit] = Future { inRelationDB.flush() }
    val f3: Future[Unit] = Future { outRelationDB.flush() }
    val f4: Future[Unit] = Future { relationTypeDB.flush() }
    Await.result(f1, Duration.Inf)
    Await.result(f2, Duration.Inf)
    Await.result(f3, Duration.Inf)
    Await.result(f4, Duration.Inf)
    true
  }

  private def _wrapEdge(lineArr: Array[String]) = {
    val relId: Long = lineArr(idIndex).toLong
    val fromId: Long = lineArr(fromIdIndex).toLong
    val toId: Long = lineArr(toIdIndex).toLong
    val edgeType: Int = PDBMetaData.getTypeId(lineArr(labelIndex))
    val propMap: Map[Int, Any] = _getPropMap(lineArr, propHeadMap)

    (relId, fromId, toId, edgeType, propMap)
  }

  override protected def _commitInnerFileStatToGlobal(): Boolean = {
    innerFileRelCountByType.foreach(kv => globalArgs.importerStatics.relTypeCountAdd(kv._1, kv._2))
    true
  }

}
