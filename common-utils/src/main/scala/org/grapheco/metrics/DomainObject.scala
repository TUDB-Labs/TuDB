package org.grapheco.metrics

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
