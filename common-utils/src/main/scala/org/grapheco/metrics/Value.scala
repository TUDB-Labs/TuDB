package org.grapheco.metrics

class Value(v: Any) {
  var value: Any = v

  def -(other: Value): Value = {
    var succ: Boolean = false
    var newVal: Value = new Value()
    if (value.isInstanceOf[Int] && other.isInstanceOf[Int]) {
      newVal.setValue(value.asInstanceOf[Int] - other.asInstanceOf[Int])
      succ = true
    } else if (value.isInstanceOf[Float] && other.isInstanceOf[Float]) {
      newVal.setValue(value.asInstanceOf[Float] - other.asInstanceOf[Float])
      succ = true
    }

    if (succ) {
      return newVal
    }
    null
  }

  def setValue(v: Any): Unit = {
    value = v
  }

  override def toString(): String = {
    value.toString
  }
}
