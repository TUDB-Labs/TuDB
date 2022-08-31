package org.grapheco.metrics

class Record(l: Label, v: Value) {
  var timestamp: Timestamp = new Timestamp()
  var label: Label = l
  var value: Value = v

  def matchLabel(r: Record): Boolean = {
    label.matches(r.label)
  }

  def containLabel(l: Label): Boolean = {
    label.contains(l)
  }

  def -(r: Record): Record = {
    val v = value - r.value
    if (v == null) {
      return null
    }
    value = v
    this
  }

  def computeLatency(r: Record): Long = {
    timestamp - r.timestamp
  }

  def print(dID: String): Unit = {
    printf("[%s][%s][%s]%s\n", dID, label.toString(), timestamp.toString(), value.toString())
  }
}
