package org.grapheco.lynx.operator

import org.apache.commons.collections4.CollectionUtils
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.property.{LynxInteger, LynxString}
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey}
import org.junit.Test
import org.opencypher.v9_0.expressions.{ListLiteral, Property, PropertyKeyName, SignedDecimalIntegerLiteral, Variable}

import scala.collection.JavaConverters._

/**
  *@description:
  */
class UnwindOperatorTest extends BaseOperatorTest {
  val node1 = TestNode(
    TestId(1L),
    Seq(LynxNodeLabel("Person")),
    Map(
      LynxPropertyKey("name") -> LynxValue("Alex"),
      LynxPropertyKey("lst") -> LynxList(List(1, 2, 3).map(LynxInteger(_)))
    )
  )
  val node2 = TestNode(
    TestId(2L),
    Seq(LynxNodeLabel("Person")),
    Map(LynxPropertyKey("name") -> LynxValue("Bob"))
  )
  val node3 = TestNode(
    TestId(3L),
    Seq(LynxNodeLabel("Person")),
    Map(
      LynxPropertyKey("name") -> LynxValue("Cat"),
      LynxPropertyKey("lst") -> LynxList(
        List(
          LynxList(List(1, 2, 3).map(LynxInteger(_))),
          LynxList(List(4, 5, 6).map(LynxInteger(_)))
        )
      )
    )
  )

  all_nodes.append(node1, node2, node3)

  @Test
  def testNoInUnwind(): Unit = {
    val expr = ListLiteral(
      Seq(
        SignedDecimalIntegerLiteral("1")(defaultPosition),
        SignedDecimalIntegerLiteral("2")(defaultPosition),
        SignedDecimalIntegerLiteral("3")(defaultPosition)
      )
    )(defaultPosition)

    val operator = UnwindOperator(
      None,
      Variable("n")(defaultPosition),
      expr,
      expressionEvaluator,
      ctx.expressionContext
    )
    val res =
      getOperatorAllOutputs(operator).map(f => f.batchData.flatten).map(f => f.asJava).toList.asJava
    CollectionUtils.isEqualCollection(
      List(
        Seq(LynxInteger(1)).asJava,
        Seq(LynxInteger(2)).asJava,
        Seq(LynxInteger(3)).asJava
      ).asJava,
      res
    )
  }

  @Test
  def testNoInUnwindWithNested(): Unit = {
    val expr = ListLiteral(
      Seq(
        ListLiteral(
          Seq(
            SignedDecimalIntegerLiteral("1")(defaultPosition),
            SignedDecimalIntegerLiteral("2")(defaultPosition),
            SignedDecimalIntegerLiteral("3")(defaultPosition)
          )
        )(defaultPosition),
        ListLiteral(
          Seq(
            SignedDecimalIntegerLiteral("4")(defaultPosition),
            SignedDecimalIntegerLiteral("5")(defaultPosition),
            SignedDecimalIntegerLiteral("6")(defaultPosition)
          )
        )(defaultPosition)
      )
    )(defaultPosition)

    val operator = UnwindOperator(
      None,
      Variable("n")(defaultPosition),
      expr,
      expressionEvaluator,
      ctx.expressionContext
    )
    val res =
      getOperatorAllOutputs(operator).map(f => f.batchData.flatten).map(f => f.asJava).toList.asJava
    CollectionUtils.isEqualCollection(
      List(
        Seq(LynxInteger(1), LynxInteger(2), LynxInteger(3)).asJava,
        Seq(LynxInteger(4), LynxInteger(5), LynxInteger(6)).asJava
      ).asJava,
      res
    )
  }
  @Test
  def testInUnwind(): Unit = {
    val in = prepareNodeScanOperator("n", Seq.empty, Seq.empty)
    val expr = Property(Variable("n")(defaultPosition), PropertyKeyName("lst")(defaultPosition))(
      defaultPosition
    )
    val unwindOp = UnwindOperator(
      Option(in),
      Variable("x")(defaultPosition),
      expr,
      expressionEvaluator,
      ctx.expressionContext
    )
    val selectOp =
      SelectOperator(Seq(("x", Option("x"))), unwindOp, expressionEvaluator, ctx.expressionContext)
    val res =
      getOperatorAllOutputs(selectOp).map(f => f.batchData.flatten).map(f => f.asJava).toList.asJava
    CollectionUtils.isEqualCollection(
      List(
        Seq(LynxInteger(1), LynxInteger(2), LynxInteger(3)).asJava,
        Seq(null).asJava,
        Seq(
          LynxList(List(1, 2, 3).map(LynxInteger(_))),
          LynxList(List(4, 5, 6).map(LynxInteger(_)))
        ).asJava
      ).asJava,
      res
    )
  }
}
