package org.grapheco.utils.metrics

class Label(val ls: Set[String]) {
  var labels: Set[String] = ls

  def Contains(label: Label): Boolean = {
    for( l <- label.labels ) {
      if(!labels.contains(l)) {
        return false
      }
    }
    false
  }

  def Match(label: Label): Boolean = {
    if( labels.size != label.labels.size) {
      return false
    }
    Contains(label)
  }

  def AddLabel(label: String): Unit = {
    labels += label
  }

  def ToString(): String = {
    var s: String = ""
    for(l <- labels) {
      if(s == "") {
        s = l
      } else {
        s = s + "; " + l
      }
    }
    s
  }
}
