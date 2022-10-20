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

import org.grapheco.lynx.expression.{LynxProperty, LynxVariable}
import org.grapheco.lynx.procedure.ProcedureExpression
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.property.{LynxNull, LynxString}
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey, LynxRelationshipType}
import org.junit.{Assert, Test}
import org.opencypher.v9_0.ast.{RemoveLabelItem, RemovePropertyItem}
import org.opencypher.v9_0.expressions.{FunctionInvocation, FunctionName, LabelName, Namespace, Property, PropertyKeyName, StringLiteral, Variable}

/**
  *@description:
  */
class RemoveOperatorTest() extends BaseOperatorTest {
  val node1 = TestNode(
    TestId(1L),
    Seq(LynxNodeLabel("Swedish")),
    Map(LynxPropertyKey("name") -> LynxValue("Andy"), LynxPropertyKey("age") -> LynxValue(36))
  )
  val node2 = TestNode(
    TestId(2L),
    Seq(LynxNodeLabel("Swedish"), LynxNodeLabel("German")),
    Map(LynxPropertyKey("name") -> LynxValue("Peter"), LynxPropertyKey("age") -> LynxValue(34))
  )
  val node3 = TestNode(
    TestId(3L),
    Seq(LynxNodeLabel("Swedish")),
    Map(LynxPropertyKey("name") -> LynxValue("Timothy"), LynxPropertyKey("age") -> LynxValue(25))
  )

  val rel1 = TestRelationship(
    TestId(1L),
    TestId(1L),
    TestId(2L),
    Option(LynxRelationshipType("KNOWS")),
    Map.empty
  )
  val rel2 = TestRelationship(
    TestId(1L),
    TestId(1L),
    TestId(3L),
    Option(LynxRelationshipType("KNOWS")),
    Map.empty
  )
  all_nodes.append(node1, node2, node3)
  all_rels.append(rel1, rel2)

  @Test
  def testRemoveAProperty(): Unit = {
    val removeItem = Seq(
      RemovePropertyItem(
        Property(Variable("a")(defaultPosition), PropertyKeyName("age")(defaultPosition))(
          defaultPosition
        )
      )
    )
    val nodeScanOperator = prepareNodeScanOperator(
      "a",
      Seq.empty,
      Seq((PropertyKeyName("name")(defaultPosition), StringLiteral("Andy")(defaultPosition)))
    )
    val removeOperator = RemoveOperator(
      nodeScanOperator,
      removeItem,
      model,
      expressionEvaluator,
      ctx.expressionContext
    )
    val projectColumn = Seq(
      (
        "a.name",
        Property(
          Variable("a")(defaultPosition),
          PropertyKeyName("name")(defaultPosition)
        )(defaultPosition)
      ),
      (
        "a.age",
        Property(
          Variable("a")(defaultPosition),
          PropertyKeyName("age")(defaultPosition)
        )(defaultPosition)
      )
    )
    val projectOperator =
      ProjectOperator(removeOperator, projectColumn, expressionEvaluator, ctx.expressionContext)

    val res = getOperatorAllOutputs(projectOperator).head.batchData.flatten
    Assert.assertEquals(Seq(LynxString("Andy"), LynxNull), res)
  }

  @Test
  def testRemoveALabelFromNode(): Unit = {
    val removeItems = Seq(
      RemoveLabelItem(Variable("n")(defaultPosition), Seq(LabelName("German")(defaultPosition)))(
        defaultPosition
      )
    )
    val nodeScanOperator = prepareNodeScanOperator(
      "n",
      Seq.empty,
      Seq((PropertyKeyName("name")(defaultPosition), StringLiteral("Peter")(defaultPosition)))
    )
    val removeOperator =
      RemoveOperator(
        nodeScanOperator,
        removeItems,
        model,
        expressionEvaluator,
        ctx.expressionContext
      )

    val namespace = List.empty
    val funcName = "labels"
    val procedure = runnerContext.procedureRegistry.getProcedure(namespace, funcName).get

    val projectColumn = Seq(
      (
        "n.name",
        Property(
          Variable("n")(defaultPosition),
          PropertyKeyName("name")(defaultPosition)
        )(defaultPosition)
      ),
      (
        "labels",
        ProcedureExpression(
          procedure,
          IndexedSeq(LynxVariable("n")),
          false,
          funcName,
          null,
          false
        )
      )
    )
    val projectOperator =
      ProjectOperator(removeOperator, projectColumn, expressionEvaluator, ctx.expressionContext)

    val res = getOperatorAllOutputs(projectOperator).head.batchData.flatten
    Assert.assertEquals(
      Seq(LynxString("Peter"), LynxList(List(LynxString("Swedish")))),
      res
    )
  }
  @Test
  def testRemoveMultipleLabelsFromNode(): Unit = {
    val removeItems = Seq(
      RemoveLabelItem(
        Variable("n")(defaultPosition),
        Seq(LabelName("German")(defaultPosition), LabelName("Swedish")(defaultPosition))
      )(
        defaultPosition
      )
    )
    val nodeScanOperator = prepareNodeScanOperator(
      "n",
      Seq.empty,
      Seq((PropertyKeyName("name")(defaultPosition), StringLiteral("Peter")(defaultPosition)))
    )
    val removeOperator =
      RemoveOperator(
        nodeScanOperator,
        removeItems,
        model,
        expressionEvaluator,
        ctx.expressionContext
      )

    val namespace = List.empty
    val funcName = "labels"
    val procedure = runnerContext.procedureRegistry.getProcedure(namespace, funcName).get

    val projectColumn = Seq(
      (
        "n.name",
        Property(
          Variable("n")(defaultPosition),
          PropertyKeyName("name")(defaultPosition)
        )(defaultPosition)
      ),
      (
        "labels",
        ProcedureExpression(
          procedure,
          IndexedSeq(LynxVariable("n")),
          false,
          funcName,
          null,
          false
        )
      )
    )
    val projectOperator =
      ProjectOperator(removeOperator, projectColumn, expressionEvaluator, ctx.expressionContext)

    val res = getOperatorAllOutputs(projectOperator).head.batchData.flatten
    Assert.assertEquals(
      Seq(LynxString("Peter"), LynxList(List())),
      res
    )
  }
}
