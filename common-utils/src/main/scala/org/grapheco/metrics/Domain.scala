package org.grapheco.metrics

import util.control.Breaks

class Domain(dID: String) {
  var records: Vector[Record] = Vector()
  val id: String = dID

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
        printRecord(r)
      }
    }
  }

  def printRecord(r: Record): Unit = {
    printf("[%s]%s", dID, r.toString())
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
