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

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey}
import org.junit.{Assert, Test}
import org.opencypher.v9_0.ast.{AscSortItem, DescSortItem}
import org.opencypher.v9_0.expressions.{Property, PropertyKeyName, Variable}

/**
  *@author:John117
  *@createDate:2022/8/8
  *@description:
  */
class OrderByOperatorTest extends BaseOperatorTest {
  val node1 = TestNode(
    TestId(1L),
    Seq(LynxNodeLabel("Person")),
    Map(LynxPropertyKey("name") -> LynxValue("Alex"))
  )
  val node2 = TestNode(
    TestId(2L),
    Seq(LynxNodeLabel("Person")),
    Map(LynxPropertyKey("name") -> LynxValue("Bob"))
  )
  val node3 = TestNode(
    TestId(3L),
    Seq(LynxNodeLabel("Person")),
    Map(LynxPropertyKey("name") -> LynxValue("Cat"), LynxPropertyKey("age") -> LynxInteger(10))
  )
  val node4 = TestNode(
    TestId(4L),
    Seq(LynxNodeLabel("Person")),
    Map(LynxPropertyKey("name") -> LynxValue("Cat"), LynxPropertyKey("age") -> LynxInteger(15))
  )

  all_nodes.append(node1, node2, node3, node4)

  @Test
  def testSortDataWithDescAndAsc(): Unit = {
    val nodeScanOperator = prepareNodeScanOperator("n", Seq("Person"), Seq.empty)

    val sortItems = Seq(
      DescSortItem(
        Property(Variable("n")(defaultPosition), PropertyKeyName("name")(defaultPosition))(
          defaultPosition
        )
      )(defaultPosition),
      AscSortItem(
        Property(Variable("n")(defaultPosition), PropertyKeyName("age")(defaultPosition))(
          defaultPosition
        )
      )(defaultPosition)
    )

    val sortOperator = OrderByOperator(
      nodeScanOperator,
      sortItems,
      expressionEvaluator,
      ctx.expressionContext
    )
    val res = getOperatorAllOutputs(sortOperator)
    Assert.assertEquals(
      List(node3, node4, node2, node1),
      res.flatMap(f => f.batchData.flatten).toList
    )
  }

  @Test
  def testSortWithDesc(): Unit = {
    val nodeScanOperator = prepareNodeScanOperator("n", Seq("Person"), Seq.empty)

    val sortItems = Seq(
      DescSortItem(
        Property(Variable("n")(defaultPosition), PropertyKeyName("name")(defaultPosition))(
          defaultPosition
        )
      )(defaultPosition),
      DescSortItem(
        Property(Variable("n")(defaultPosition), PropertyKeyName("age")(defaultPosition))(
          defaultPosition
        )
      )(defaultPosition)
    )

    val sortOperator = OrderByOperator(
      nodeScanOperator,
      sortItems,
      expressionEvaluator,
      ctx.expressionContext
    )
    val res2 = getOperatorAllOutputs(sortOperator)
    Assert.assertEquals(
      List(node4, node3, node2, node1),
      res2.flatMap(f => f.batchData.flatten).toList
    )
  }

  @Test
  def testSortWithAsc(): Unit = {
    val nodeScanOperator = prepareNodeScanOperator("n", Seq("Person"), Seq.empty)

    val sortItems = Seq(
      AscSortItem(
        Property(Variable("n")(defaultPosition), PropertyKeyName("age")(defaultPosition))(
          defaultPosition
        )
      )(defaultPosition)
    )

    val sortOperator = OrderByOperator(
      nodeScanOperator,
      sortItems,
      expressionEvaluator,
      ctx.expressionContext
    )
    val res = getOperatorAllOutputs(sortOperator)
    Assert.assertEquals(
      List(node3, node4, node2, node1),
      res.flatMap(f => f.batchData.flatten).toList
    )
  }
}
