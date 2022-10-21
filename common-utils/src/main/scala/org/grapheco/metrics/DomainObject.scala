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

package org.grapheco.metrics

import org.grapheco.tudb.common.utils.LogUtil
import org.slf4j.LoggerFactory

import java.time.LocalDateTime
import scala.collection.mutable.Stack
import scala.util.control.Breaks._

object DomainObject {
  val domainID = "Query-Performance"
  var stackedLabels = Stack[String]()

  var domain: Domain = new Domain(domainID)
  val logger = LoggerFactory.getLogger("DomainObject")

  def addRecord(r: Record): Unit = {
    domain.addRecord(r)
  }

  def generateQueryID(query: String): String = {
    if (query.isEmpty) {
      return ""
    }
    val lines = query.trim.split(" ")

    var queryID = ""
    var nAddedLine = 0
    breakable {
      for (line <- lines) {
        val trimLine = line.trim
        if (!trimLine.isEmpty) {
          if (queryID.isEmpty) {
            queryID = trimLine
          } else {
            queryID = queryID + "-" + trimLine
          }
          nAddedLine += 1
        }
        if (nAddedLine >= 3) {
          break
        }
      }
    }
    LocalDateTime.now().toString + "-" + queryID
  }

  def recordLatency(labels: Set[String]): Unit = {
    var completeLabels = labels
    if (completeLabels == null) {
      completeLabels = Set[String]()
    }

    for (l <- stackedLabels) {
      if (!completeLabels.contains(l)) {
        completeLabels += l
      }
    }
    val r = new Record(new Label(completeLabels), new Value(0))
    domain.recordLatency(r)
  }

  def recordQuery(query: String): Unit = {
    val queryID = generateQueryID(query)
    if (queryID.isEmpty) {
      return
    }
    pushLabel(queryID)
  }

  def pushLabel(l: String): Unit = {
    for (existLabel <- stackedLabels) {
      if (existLabel == l) {
        return
      }
    }
    stackedLabels.push(l)
  }

  def popLabel(): String = {
    stackedLabels.pop()
  }

  def clearLabels(): Unit = {
    stackedLabels.clear()
  }

  def clearRecords(): Unit = {
    domain.records = Vector()
  }

  def printRecords(labels: Option[Set[String]]): Unit = {
    var newLabels: Set[String] = Set()
    if (labels.isEmpty) {
      for (l <- stackedLabels) {
        newLabels += l
      }
    } else {
      newLabels = labels.get
    }
    domain.printRecordByLabel(new Label(newLabels))
  }
}
