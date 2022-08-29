package org.grapheco.utils.metrics

class Record(l: Label, v: Value) {
  var timestamp: Timestamp = new Timestamp()
  var label: Label = l
  var value: Value = v

  def MatchLabel(r: Record): Boolean = {
    label.Match(r.label)
  }

  def ContainLabel(l: Label): Boolean = {
    label.Contains(l)
  }

  def -(r: Record): Record = {
    val v = value - r.value
    if(v == null) {
      return null
    }
    value = v
    this
  }

  def Print(dID: String): Unit ={
    printf("[%s][%s][%s]%s\n", dID, label.ToString(), timestamp.ToString(), value.ToString())
  }
}
