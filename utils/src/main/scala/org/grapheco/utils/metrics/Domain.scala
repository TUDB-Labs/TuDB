package org.grapheco.utils.metrics

import scala.util.control.Breaks
import scala.util.control.Breaks._

class Domain(dID: String) {
  var records: Vector[Record] = Vector()
  val id: String = dID

  def AddRecord(r: Record): Unit = {
    records = records :+ r
  }

  def AddRecordForLatency(r: Record): Unit = {
    val breakloop = new Breaks
    val continueloop = new Breaks

    breakloop.breakable {
      for (or <- records.reverse) {
        continueloop.breakable {
          if (or.MatchLabel(r)) {
            val newR = r - or
            if (newR == null) {
              continueloop.break
            }
            records = records.filterNot(_ == or)
            records = records :+ newR
            breakloop.break
          }
        }
      }
    }
  }

  def PrintRecordByLabel(l: Label): Unit = {
    for(r <- records) {
      if(r.ContainLabel(l)) {
        r.Print(id)
      }
    }
  }
}
