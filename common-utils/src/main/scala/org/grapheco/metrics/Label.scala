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

class Label(val ls: Set[String]) {
  var labels: Set[String] = ls

  def matches(label: Label): Boolean = {
    if (labels.size != label.labels.size) {
      return false
    }
    contains(label)
  }

  def contains(label: Label): Boolean = {
    if (label.labels.size == 0) {
      return true
    }
    if (labels.size == 0) {
      return false
    }

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
