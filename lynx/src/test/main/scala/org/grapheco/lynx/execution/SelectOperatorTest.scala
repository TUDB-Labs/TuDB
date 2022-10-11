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

package org.grapheco.lynx.execution

import org.apache.commons.collections4.CollectionUtils
import org.grapheco.lynx.RowBatch
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey}
import org.junit.{Assert, Test}
import org.opencypher.v9_0.expressions.{LabelName, MapExpression, NodePattern, Variable}
import org.opencypher.v9_0.util.InputPosition

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

/**
  *@author:John117
  *@createDate:2022/8/4
  *@description:
  */
class SelectOperatorTest extends BaseOperatorTest {
  val node1 = TestNode(
    TestId(1L),
    Seq(LynxNodeLabel("Person")),
    Map(LynxPropertyKey("name") -> LynxValue("Alex"), LynxPropertyKey("age") -> LynxInteger(10))
  )
  val node2 = TestNode(
    TestId(2L),
    Seq(LynxNodeLabel("Person")),
    Map(LynxPropertyKey("name") -> LynxValue("Bob"), LynxPropertyKey("age") -> LynxInteger(15))
  )
  val node3 = TestNode(
    TestId(3L),
    Seq(LynxNodeLabel("Person")),
    Map(LynxPropertyKey("name") -> LynxValue("Cat"), LynxPropertyKey("age") -> LynxInteger(20))
  )
  all_nodes.append(node1, node2, node3)

  @Test
  def testSelect(): Unit = {
    val nodeScanOperator = prepareNodeScanOperator()
    val selectOperator = SelectOperator(
      nodeScanOperator,
      Seq(("n", Option("n"))),
      expressionEvaluator,
      ctx.expressionContext
    )

    val result = ArrayBuffer[RowBatch]()
    selectOperator.open()
    var data = selectOperator.getNext()
    while (data.batchData.nonEmpty) {
      result.append(data)
      data = selectOperator.getNext()
    }
    selectOperator.close()

    Assert.assertEquals(3, result.length)
    Assert.assertTrue(
      CollectionUtils.isEqualCollection(
        List(node1, node2, node3).asJava,
        result.flatMap(rowBatch => rowBatch.batchData.flatten).toList.asJava
      )
    )
  }

  def prepareNodeScanOperator(): NodeScanOperator = {
    val variable = Option(Variable("n")(InputPosition(0, 0, 0)))
    val labels = Seq(LabelName("Person")(InputPosition(0, 0, 0)))
    val propertiesExpression = Option(
      MapExpression(
        Seq.empty
      )(InputPosition(0, 0, 0))
    )
    val pattern = NodePattern(variable, labels, propertiesExpression)(InputPosition(0, 0, 0))
    val operator = NodeScanOperator(pattern, model, expressionEvaluator, ctx.expressionContext)
    operator
  }
}
