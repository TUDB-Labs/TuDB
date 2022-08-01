package org.grapheco.lynx.operator

import org.grapheco.lynx.operator.{NodeScanOperator}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey}
import org.junit.{Assert, Test}
import org.opencypher.v9_0.expressions.{LabelName, LogicalVariable, MapExpression, NodePattern, PropertyKeyName, StringLiteral, Variable}
import org.opencypher.v9_0.util.InputPosition

/**
  *@author:John117
  *@createDate:2022/7/30
  *@description:
  */
class NodeScanOperatorTest extends BaseOperatorTest {
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
  all_nodes.append(node1, node2, node3)

  @Test
  def testScanAllNodeWithFilter(): Unit = {
    val variable = Option(Variable("n")(InputPosition(0, 0, 0)))
    val labels = Seq(LabelName("Person")(InputPosition(0, 0, 0)))
    val propertiesExpression = Option(
      MapExpression(
        Seq(
          (
            PropertyKeyName("name")(InputPosition(0, 0, 0)),
            StringLiteral("Cat")(InputPosition(0, 0, 0))
          )
        )
      )(InputPosition(0, 0, 0))
    )
    val pattern = NodePattern(variable, labels, propertiesExpression)(InputPosition(0, 0, 0))
    val operator = NodeScanOperator(pattern, model, expressionEvaluator, ctx.expressionContext)

    operator.open()
    var count = 0
    while (operator.hasNext()) {
      operator.getNext()
      count += 1
    }
    operator.close()

    Assert.assertEquals(1, count)
  }

  @Test
  def testScanAllNodeWithoutFilter(): Unit = {
    val variable = Option(Variable("n")(InputPosition(0, 0, 0)))
    val labels = Seq(LabelName("Person")(InputPosition(0, 0, 0)))
    val propertiesExpression = Option(
      MapExpression(
        Seq.empty
      )(InputPosition(0, 0, 0))
    )
    val pattern = NodePattern(variable, labels, propertiesExpression)(InputPosition(0, 0, 0))
    val operator = NodeScanOperator(pattern, model, expressionEvaluator, ctx.expressionContext)

    operator.open()
    var count = 0
    while (operator.hasNext()) {
      operator.getNext()
      count += 1
    }
    operator.close()

    Assert.assertEquals(3, count)
  }

}
