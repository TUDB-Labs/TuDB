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

import java.io.File
import java.util.concurrent._
import scala.collection.mutable.ListBuffer

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 16:39 2020/12/22
  * @Modified By:
  */
trait ReaderMode {}
case class WithHead() extends ReaderMode
case class WithOutHead() extends ReaderMode

class ImporterFileReader(
    file: File,
    delimeter: String,
    batchSize: Int = 500000,
    mode: ReaderMode = WithHead()) {

  val fileIter: Iterator[CSVLine] =
    this.synchronized(new CSVReader(file, delimeter).getAsCSVLines)

  private val _head: CSVLine = {
    mode match {
      case WithHead()    => fileIter.next()
      case WithOutHead() => new CSVLine(Array(""))
    }
  }

  val supposedQueueLength: Int = Runtime.getRuntime().availableProcessors() / 2
  var batchQueue: BlockingQueue[List[CSVLine]] =
    new LinkedBlockingQueue[List[CSVLine]](supposedQueueLength)

  val fileReaderService: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor()

  val fillQueue = new Runnable {
    override def run(): Unit = {
      batchQueue.put(_prepareBatch)
      if (!fileIter.hasNext) fileReaderService.shutdown()
    }
  }
  fileReaderService.scheduleWithFixedDelay(
    fillQueue,
    0,
    50,
    TimeUnit.MILLISECONDS
  )

  private def _prepareBatch: List[CSVLine] = {
    this.synchronized {
      var innercount = 0
      val listBuf: ListBuffer[CSVLine] = new ListBuffer[CSVLine]()
      while (fileIter.hasNext && innercount < batchSize) {
        listBuf.append(fileIter.next())
        innercount += 1
      }
      listBuf.toList
    }
  }

  def getHead: CSVLine = {
    _head
  }

  def getLines: List[CSVLine] = {
    this.synchronized {
      if (batchQueue.isEmpty) List[CSVLine]()
      else batchQueue.take()
    }
  }

  def notFinished: Boolean = {
    this.synchronized(fileIter.hasNext || batchQueue.size() > 0)
  }
}
