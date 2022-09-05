package org.grapheco.metrics

class Label(val ls: Set[String]) {
  var labels: Set[String] = ls

  def matches(label: Label): Boolean = {
    if (labels.size != label.labels.size) {
      return false
    }
    contains(label)
  }

  def contains(label: Label): Boolean = {
    for (l <- label.labels) {
      if (!labels.contains(l)) {
        return false
      }
    }
    true
  }

  def addLabel(label: String): Unit = {
    labels += label
  }

  override def toString(): String = {
    labels.mkString(";")
  }
}
