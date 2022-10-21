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

import org.junit.{Before, Test}

import scala.Console.println

class DomainObjectTest {
  val labelStr1 = "label1"
  val labelStr2 = "label2"
  val labelStr3 = "label3"

  @Before
  def init(): Unit = {
    DomainObject.clearLabels()
    DomainObject.clearRecords()
  }

  @Test
  def testLabelPushAndPop(): Unit = {
    DomainObject.pushLabel(labelStr1)
    DomainObject.pushLabel(labelStr2)
    assert(DomainObject.stackedLabels.length == 2)

    DomainObject.popLabel()
    DomainObject.pushLabel(labelStr3)
    assert(DomainObject.stackedLabels.length == 2)

    DomainObject.pushLabel(labelStr1)
    assert(DomainObject.stackedLabels.length == 2)
  }

  @Test
  def testAddRecord(): Unit = {
    DomainObject.pushLabel(labelStr1)
    DomainObject.recordLatency(Set(labelStr2))

    assert(DomainObject.domain.records.length == 1)
    var r = DomainObject.domain.records(0)
    assert(r.label.matches(new Label(Set(labelStr1, labelStr2))))

    DomainObject.pushLabel(labelStr2)
    DomainObject.recordLatency(null)
    assert(DomainObject.domain.records.length == 1)
    r = DomainObject.domain.records(0)
    assert(r.label.matches(new Label(Set(labelStr1, labelStr2))))
  }

  @Test
  def testGenerateQueryID(): Unit = {
    var id = DomainObject.generateQueryID("")
    assert(id.isEmpty)

    id = DomainObject.generateQueryID("  A B\n C D")
    assert(id.endsWith("A-B-C"))
  }

}
