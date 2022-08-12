package org.grapheco.lynx.operator

import org.apache.commons.collections4.CollectionUtils
import org.grapheco.lynx.procedure.ProcedureExpression
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.{LynxInteger, LynxString}
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey}
import org.junit.{Assert, Test}
import org.opencypher.v9_0.ast.AliasedReturnItem
import org.opencypher.v9_0.expressions.{CountStar, FunctionInvocation, FunctionName, Namespace, Property, PropertyKeyName, Variable}

import scala.collection.JavaConverters._

/**
  *@author:John117
  *@createDate:2022/8/9
  *@description:
  */
class AggregationOperatorTest extends BaseOperatorTest {
  val node1 = TestNode(
    TestId(1L),
    Seq(LynxNodeLabel("Person")),
    Map(LynxPropertyKey("name") -> LynxValue("Alex"))
  )
  val node2 = TestNode(
    TestId(2L),
    Seq(LynxNodeLabel("Person")),
    Map(LynxPropertyKey("name") -> LynxValue("Alex"), LynxPropertyKey("age") -> LynxInteger(10))
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
  def testCountNode(): Unit = {
    val nodeOperator = prepareNodeScanOperator("n", Seq.empty, Seq.empty)

    val groupExpr = Seq.empty

    val aggregationExpr = Seq(
      AliasedReturnItem(CountStar()(defaultPosition), Variable("count(*)")(defaultPosition))(
        defaultPosition
      )
    )

    val groupByOperator = AggregationOperator(
      aggregationExpr,
      groupExpr,
      nodeOperator,
      expressionEvaluator,
      ctx.expressionContext
    )

    val result = getOperatorAllOutputs(groupByOperator).flatMap(f => f.batchData).head.head.value
    Assert.assertEquals(4L, result)
  }

  @Test
  def testCountByNode(): Unit = {
    val nodeOperator = prepareNodeScanOperator("n", Seq.empty, Seq.empty)

    val groupExpr = Seq(
      AliasedReturnItem(Variable("n")(defaultPosition), Variable("n")(defaultPosition))(
        defaultPosition
      )
    )
    val aggregationExpr = Seq(
      AliasedReturnItem(CountStar()(defaultPosition), Variable("count(*)")(defaultPosition))(
        defaultPosition
      )
    )

    val groupByOperator = AggregationOperator(
      aggregationExpr,
      groupExpr,
      nodeOperator,
      expressionEvaluator,
      ctx.expressionContext
    )

    val result = getOperatorAllOutputs(groupByOperator)
      .flatMap(f => f.batchData)
      .map(f => f.asJava)
      .toSeq
      .asJava
    Assert.assertTrue(
      CollectionUtils.isEqualCollection(
        List(
          List(node1, LynxInteger(1L)).asJava,
          List(node2, LynxInteger(1L)).asJava,
          List(node3, LynxInteger(1L)).asJava,
          List(node4, LynxInteger(1L)).asJava
        ).asJava,
        result
      )
    )
  }

  @Test
  def testCountByProperty(): Unit = {
    val nodeOperator = prepareNodeScanOperator("n", Seq.empty, Seq.empty)

    val groupExpr = Seq(
      AliasedReturnItem(
        Property(Variable("n")(defaultPosition), PropertyKeyName("name")(defaultPosition))(
          defaultPosition
        ),
        Variable("n.name")(defaultPosition)
      )(
        defaultPosition
      )
    )
    val aggregationExpr = Seq(
      AliasedReturnItem(CountStar()(defaultPosition), Variable("count(*)")(defaultPosition))(
        defaultPosition
      )
    )

    val groupByOperator = AggregationOperator(
      aggregationExpr,
      groupExpr,
      nodeOperator,
      expressionEvaluator,
      ctx.expressionContext
    )

    val result = getOperatorAllOutputs(groupByOperator)
      .flatMap(f => f.batchData)
      .map(f => f.asJava)
      .toSeq
      .asJava
    Assert.assertTrue(
      CollectionUtils.isEqualCollection(
        List(
          List(LynxString("Alex"), LynxInteger(2L)).asJava,
          List(LynxString("Cat"), LynxInteger(2L)).asJava
        ).asJava,
        result
      )
    )
  }
  @Test
  def testCountByMultiplePropertiesWithLynxNull(): Unit = {
    val nodeOperator = prepareNodeScanOperator("n", Seq.empty, Seq.empty)
    val groupExpr = Seq.empty
    val aggregationExpr = Seq(
      AliasedReturnItem(
        ProcedureExpression(
          FunctionInvocation(
            Namespace(List.empty)(defaultPosition),
            FunctionName("count")(defaultPosition),
            false,
            IndexedSeq(
              Property(Variable("n")(defaultPosition), PropertyKeyName("name")(defaultPosition))(
                defaultPosition
              )
            )
          )(defaultPosition)
        )(runnerContext),
        Variable("count(n.name)")(defaultPosition)
      )(defaultPosition),
      AliasedReturnItem(
        ProcedureExpression(
          FunctionInvocation(
            Namespace(List.empty)(defaultPosition),
            FunctionName("count")(defaultPosition),
            false,
            IndexedSeq(
              Property(Variable("n")(defaultPosition), PropertyKeyName("age")(defaultPosition))(
                defaultPosition
              )
            )
          )(defaultPosition)
        )(runnerContext),
        Variable("count(n.age)")(defaultPosition)
      )(defaultPosition)
    )

    val groupByOperator = AggregationOperator(
      aggregationExpr,
      groupExpr,
      nodeOperator,
      expressionEvaluator,
      ctx.expressionContext
    )

    val result = getOperatorAllOutputs(groupByOperator)
      .flatMap(f => f.batchData)
      .map(f => f.asJava)
      .toSeq
      .asJava
    Assert.assertTrue(
      CollectionUtils.isEqualCollection(
        List(
          List(LynxInteger(4L), LynxInteger(3L)).asJava
        ).asJava,
        result
      )
    )
  }
}
