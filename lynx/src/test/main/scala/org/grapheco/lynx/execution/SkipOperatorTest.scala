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
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.{LynxInteger}
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey}
import org.junit.{Assert, Test}
import org.opencypher.v9_0.expressions.SignedDecimalIntegerLiteral
import scala.collection.JavaConverters._

class SkipOperatorTest extends BaseOperatorTest {
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
  def testSkipData(): Unit = {
    val nodeScanOperator = prepareNodeScanOperator("n", Seq.empty, Seq.empty)
    val skipOperator =
      SkipOperator(nodeScanOperator, 2, expressionEvaluator, ctx.expressionContext)
    val result = getOperatorAllOutputs(skipOperator).flatMap(f => f.batchData.flatten).toList.asJava
    Assert.assertTrue(
      CollectionUtils.isEqualCollection(
        List(
          node3,
          node4
        ).asJava,
        result
      )
    )
  }
  @Test
  def testSkipLargeThanDataSize(): Unit = {
    val nodeScanOperator = prepareNodeScanOperator("n", Seq.empty, Seq.empty)
    val skipOperator =
      SkipOperator(nodeScanOperator, 20000, expressionEvaluator, ctx.expressionContext)
    val result = getOperatorAllOutputs(skipOperator).flatMap(f => f.batchData.flatten).toList.asJava
    Assert.assertTrue(
      CollectionUtils.isEqualCollection(
        List.empty.asJava,
        result
      )
    )
  }
}
