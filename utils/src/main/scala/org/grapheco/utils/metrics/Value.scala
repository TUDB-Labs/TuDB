package org.grapheco.utils.metrics

class Value(v: Any) {
  var value: Any = v

  def -(other: Value) : Value = {
    var succ: Boolean = false
    var newVal: Value = new Value()
    if(value.isInstanceOf[Int] && other.isInstanceOf[Int]) {
      newVal.SetValue(value.asInstanceOf[Int] - other.asInstanceOf[Int])
      succ = true
    } else if (value.isInstanceOf[Float] && other.isInstanceOf[Float]) {
      newVal.SetValue(value.asInstanceOf[Float] - other.asInstanceOf[Float])
      succ = true
    }

    if(succ) {
      return newVal
    }
    null
  }

  def SetValue(v: Any): Unit = {
    value = v
  }

  def ToString(): String = {
    value.toString
  }

}
