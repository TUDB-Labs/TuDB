package org.grapheco.lynx.execution

import org.apache.commons.collections4.CollectionUtils
import org.grapheco.lynx.RowBatch
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey}
import org.junit.{Assert, Test}
import org.opencypher.v9_0.expressions.{Expression, LabelName, MapExpression, NodePattern, PropertyKeyName, SignedDecimalIntegerLiteral, StringLiteral, Variable}
import org.opencypher.v9_0.util.InputPosition

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

/**
  *@author:John117
  *@createDate:2022/7/30
  *@description:
  */
class NodeScanOperatorTest extends BaseOperatorTest {
  val node1 = TestNode(
    TestId(1L),
    Seq(LynxNodeLabel("Person1")),
    Map(LynxPropertyKey("name") -> LynxValue("Alex"))
  )
  val node2 = TestNode(
    TestId(2L),
    Seq(LynxNodeLabel("Person2")),
    Map(LynxPropertyKey("name") -> LynxValue("Bob"))
  )
  val node3 = TestNode(
    TestId(3L),
    Seq(LynxNodeLabel("Person3")),
    Map(LynxPropertyKey("name") -> LynxValue("Cat"), LynxPropertyKey("age") -> LynxInteger(10))
  )
  val node4 = TestNode(
    TestId(4L),
    Seq(LynxNodeLabel("Person3")),
    Map(LynxPropertyKey("name") -> LynxValue("Cat"), LynxPropertyKey("age") -> LynxInteger(15))
  )
  all_nodes.append(node1, node2, node3, node4)

  @Test
  def testScanAllNodesWithOutFilter(): Unit = {
    val variable = Option(Variable("n")(InputPosition(0, 0, 0)))
    val labels = Seq.empty
    val propertiesExpression = Option.empty[Expression]

    val resultData = getOperatorData(variable, labels, propertiesExpression)
    Assert.assertEquals(4, resultData.length)
    Assert.assertTrue(
      CollectionUtils.isEqualCollection(
        List(node2, node1, node3, node4).asJava,
        resultData.flatMap(f => f.batchData.flatten).toList.asJava
      )
    )
  }

  @Test
  def testScanAllNodesFilteredByLabel(): Unit = {
    val variable = Option(Variable("n")(InputPosition(0, 0, 0)))
    val labels = Seq(LabelName("Person1")(InputPosition(0, 0, 0)))
    val propertiesExpression = Option.empty[Expression]

    val resultData = getOperatorData(variable, labels, propertiesExpression)
    Assert.assertEquals(1, resultData.length)
    Assert.assertEquals(node1, resultData.head.batchData.head.head)
  }

  @Test
  def testScanAllNodesFilteredByProperty(): Unit = {
    val variable = Option(Variable("n")(InputPosition(0, 0, 0)))
    val labels = Seq.empty
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

    val resultData = getOperatorData(variable, labels, propertiesExpression)

    Assert.assertEquals(2, resultData.length)
    Assert.assertTrue(
      CollectionUtils.isEqualCollection(
        List(node3, node4).asJava,
        resultData.flatMap(f => f.batchData.flatten).toList.asJava
      )
    )
  }

  @Test
  def testScanAllNodeFilteredByLabelAndProperties(): Unit = {
    val variable = Option(Variable("n")(InputPosition(0, 0, 0)))
    val labels = Seq(LabelName("Person3")(InputPosition(0, 0, 0)))
    val propertiesExpression = Option(
      MapExpression(
        Seq(
          (
            PropertyKeyName("name")(InputPosition(0, 0, 0)),
            StringLiteral("Cat")(InputPosition(0, 0, 0))
          ),
          (
            PropertyKeyName("age")(InputPosition(0, 0, 0)),
            SignedDecimalIntegerLiteral("15")(InputPosition(0, 0, 0))
          )
        )
      )(InputPosition(0, 0, 0))
    )

    val resultData = getOperatorData(variable, labels, propertiesExpression)

    Assert.assertEquals(1, resultData.length)
    Assert.assertEquals(node4, resultData.head.batchData.head.head)
  }

  def getOperatorData(
      variable: Option[Variable],
      labels: Seq[LabelName],
      propertiesExpression: Option[Expression]
    ): Array[RowBatch] = {
    val pattern = NodePattern(variable, labels, propertiesExpression)(InputPosition(0, 0, 0))
    val operator = NodeScanOperator(pattern, model, expressionEvaluator, ctx.expressionContext)

    val resultArray: ArrayBuffer[RowBatch] = ArrayBuffer()
    operator.open()
    var rowData = operator.getNext()
    while (rowData.batchData.nonEmpty) {
      resultArray.append(rowData)
      rowData = operator.getNext()
    }
    operator.close()

    resultArray.toArray
  }
}
