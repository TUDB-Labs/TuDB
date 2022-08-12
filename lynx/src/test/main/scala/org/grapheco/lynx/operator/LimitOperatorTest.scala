package org.grapheco.lynx.operator

import org.apache.commons.collections4.CollectionUtils
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey}
import org.junit.{Assert, Test}
import org.opencypher.v9_0.expressions.SignedDecimalIntegerLiteral

import scala.collection.JavaConverters._

class LimitOperatorTest extends BaseOperatorTest {
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
  def testLimitData(): Unit = {
    val nodeScanOperator = prepareNodeScanOperator("n", Seq.empty, Seq.empty)
    val skipOperator =
      LimitOperator(2, nodeScanOperator, expressionEvaluator, ctx.expressionContext)
    val result = getOperatorAllOutputs(skipOperator).flatMap(f => f.batchData.flatten).toList.asJava
    Assert.assertTrue(
      CollectionUtils.isEqualCollection(
        List(
          node1,
          node2
        ).asJava,
        result
      )
    )
  }
}
