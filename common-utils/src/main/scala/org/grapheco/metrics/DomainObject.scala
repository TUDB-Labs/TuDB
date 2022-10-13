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

import org.slf4j.LoggerFactory

import scala.collection.mutable.Stack

object DomainObject {
  val domainID = "query"
  var stackedLabels = Stack[String]()

  var domain: Domain = new Domain(domainID)

  def addRecord(r: Record): Unit = {
    domain.addRecord(r)
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

  def printRecords(inputLabelSet: Set[String]): Unit = {
    var labelSet = inputLabelSet
    if (labelSet == null) {
      labelSet = Set[String]()
    }
    val label = new Label(labelSet)
    domain.printRecordByLabel(label)
  }
}
