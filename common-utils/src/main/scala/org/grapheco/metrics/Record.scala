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

  override def toString(): String = {
    String.format("[%s][%s]%s", label.toString(), timestamp.toString(), value.toString())
  }
}
