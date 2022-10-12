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
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class Domain(dID: String) {
  var records: Vector[Record] = Vector()
  val id: String = dID
  val logger = LoggerFactory.getLogger(dID)

  def addRecord(r: Record): Unit = {
    records = records :+ r
  }

  // record the latency for an operation
  def recordLatency(r: Record): Unit = {
    // if there is an existing record with the same label, we assume this record is the start point
    // of the operation and compute the operation latency according to the timestamp
    for (preRecord <- records.reverse) {
      if (preRecord.matchLabel(r)) {
        val latency = r.computeLatency(preRecord)
        records = records.filterNot(_ == preRecord)
        r.value.setValue(latency)
        records = records :+ r
        return
      }
    }

    records = records :+ r
  }

  def printRecordByLabel(l: Label): Unit = {
    for (r <- records) {
      if (r.containLabel(l)) {
        LogUtil.info(logger, "%s", r.toString())
      }
    }
  }

  def filterRecords(l: Label): Set[Record] = {
    var filterRecords: Set[Record] = Set()
    for (r <- records) {
      if (r.containLabel(l)) {
        filterRecords += r
      }
    }
    filterRecords
  }
}
