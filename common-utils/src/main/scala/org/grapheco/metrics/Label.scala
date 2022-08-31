package org.grapheco.metrics

class Label(val ls: Set[String]) {
  var labels: Set[String] = ls

  def contains(label: Label): Boolean = {
    for (l <- label.labels) {
      if (!labels.contains(l)) {
        return false
      }
    }
    false
  }

  def matches(label: Label): Boolean = {
    if (labels.size != label.labels.size) {
      return false
    }
    contains(label)
  }

  def addLabel(label: String): Unit = {
    labels += label
  }

  override def toString(): String = {
    labels.mkString(";")
  }
}