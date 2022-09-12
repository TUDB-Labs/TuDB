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
    for (or <- records.reverse) {
      if (or.matchLabel(r)) {
        val latency = r.computeLatency(or)
        records = records.filterNot(_ == or)
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
        r.print(id)
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

  def getRecordsSize(): Int = {
    records.length
  }
}
