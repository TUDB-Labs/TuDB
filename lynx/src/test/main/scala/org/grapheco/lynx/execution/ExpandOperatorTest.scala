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
import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey, LynxRelationshipType}
import org.junit.{Assert, Test}
import org.opencypher.v9_0.expressions.{MapExpression, NodePattern, PropertyKeyName, RelTypeName, RelationshipPattern, SemanticDirection, StringLiteral}

import scala.collection.JavaConverters._

class ExpandOperatorTest extends BaseOperatorTest {
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
    Map(LynxPropertyKey("name") -> LynxValue("Cat"))
  )
  val node4 = TestNode(
    TestId(4L),
    Seq(LynxNodeLabel("Person")),
    Map(LynxPropertyKey("name") -> LynxValue("Dog"))
  )

  val rel1 = TestRelationship(
    TestId(1L),
    TestId(1L),
    TestId(2L),
    Option(LynxRelationshipType("KNOW")),
    Map(LynxPropertyKey("year") -> LynxInteger(2022))
  )
  val rel2 = TestRelationship(
    TestId(2L),
    TestId(2L),
    TestId(3L),
    Option(LynxRelationshipType("KNOW")),
    Map(LynxPropertyKey("year") -> LynxInteger(2021))
  )
  val rel3 = TestRelationship(
    TestId(3L),
    TestId(2L),
    TestId(4L),
    Option(LynxRelationshipType("KNOW")),
    Map(LynxPropertyKey("year") -> LynxInteger(2021))
  )

  all_nodes.append(node1, node2, node3, node4)
  all_rels.append(rel1, rel2, rel3)

  @Test
  def testExpand(): Unit = {
    val leftNodePropertiesExpression = Option(
      MapExpression(
        Seq(
          (
            PropertyKeyName("name")(defaultPosition),
            StringLiteral("Alex")(defaultPosition)
          )
        )
      )(defaultPosition)
    )
    val rightNodePropertiesExpression = Option(
      MapExpression(
        Seq(
          (
            PropertyKeyName("name")(defaultPosition),
            StringLiteral("Bob")(defaultPosition)
          )
        )
      )(defaultPosition)
    )

    val leftPattern = NodePattern(None, Seq.empty, leftNodePropertiesExpression)(defaultPosition)
    val rightPattern = NodePattern(None, Seq.empty, rightNodePropertiesExpression)(defaultPosition)
    val expandRightPattern = NodePattern(None, Seq.empty, Option.empty)(defaultPosition)

    val relPattern = RelationshipPattern(
      None,
      Seq(RelTypeName("KNOW")(defaultPosition)),
      None,
      None,
      SemanticDirection.OUTGOING
    )(defaultPosition)

    val pathScanOperator = PathScanOperator(
      relPattern,
      leftPattern,
      rightPattern,
      model,
      expressionEvaluator,
      ctx.expressionContext
    )

    val expandOperator = ExpandOperator(
      pathScanOperator,
      relPattern,
      expandRightPattern,
      model,
      expressionEvaluator,
      ctx.expressionContext
    )
    val res = getOperatorAllOutputs(expandOperator)
      .map(f => f.batchData.map(f => f.asJava).asJava)
      .toList
      .asJava
    Assert.assertTrue(
      CollectionUtils.isEqualCollection(
        List(
          List(
            List(node1, rel1, node2, rel2, node3).asJava,
            List(node1, rel1, node2, rel3, node4).asJava
          ).asJava
        ).asJava,
        res
      )
    )
  }
}
