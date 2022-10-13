// Copyright 2022 The TuDB Authors. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
