package org.grapheco.lynx.execution

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.property.{LynxInteger, LynxNull}
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey}
import org.junit.{Assert, Test}
import org.opencypher.v9_0.expressions.{ListLiteral, Property, PropertyKeyName, SignedDecimalIntegerLiteral, Variable}

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
  def testUnwindLiteral(): Unit = {
    /*
        unwind [1,2,3] as n
        return x
     */
    val expr = ListLiteral(
      Seq(
        SignedDecimalIntegerLiteral("1")(defaultPosition),
        SignedDecimalIntegerLiteral("2")(defaultPosition),
        SignedDecimalIntegerLiteral("3")(defaultPosition)
      )
    )(defaultPosition)

    val literalOperator =
      LiteralOperator(Seq("n"), Seq(expr), expressionEvaluator, ctx.expressionContext)

    val operator = UnwindOperator(
      literalOperator,
      Seq("n"),
      expressionEvaluator,
      ctx.expressionContext
    )
    val res = getOperatorFlattenResult(operator)
    Assert.assertEquals(
      List(LynxInteger(1), LynxInteger(2), LynxInteger(3)),
      res
    )
  }

  @Test
  def testUnwindLiterals(): Unit = {
    /*
        unwind [1,2,3] as n, [4, 5, 6] as m
        return n, m
     */
    val exprs = Seq(
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

    val literalOperator =
      LiteralOperator(Seq("n", "m"), exprs, expressionEvaluator, ctx.expressionContext)

    val operator = UnwindOperator(
      literalOperator,
      Seq("n", "m"),
      expressionEvaluator,
      ctx.expressionContext
    )
    val res = getOperatorAllOutputs(operator).flatMap(rowBatch => rowBatch.batchData).toList
    Assert.assertEquals(
      List(
        List(LynxInteger(1), LynxInteger(2), LynxInteger(3)),
        List(LynxInteger(4), LynxInteger(5), LynxInteger(6))
      ),
      res
    )

  }

  @Test
  def testUnwindWithNested(): Unit = {
    /*
        unwind [ [1,2,3], [4,5,6] ] as n
        return n
     */
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

    val literalOperator =
      LiteralOperator(Seq("n"), Seq(expr), expressionEvaluator, ctx.expressionContext)

    val operator = UnwindOperator(
      literalOperator,
      Seq("n"),
      expressionEvaluator,
      ctx.expressionContext
    )
    val res = getOperatorFlattenResult(operator)
    Assert.assertEquals(
      List(
        LynxList(List(LynxInteger(1), LynxInteger(2), LynxInteger(3))),
        LynxList(List(LynxInteger(4), LynxInteger(5), LynxInteger(6)))
      ),
      res
    )
  }

  @Test
  def testUnwindNoneLiteralInput(): Unit = {
    /*
        match (n)
        unwind n.lst as n.lst
        return n.lst
     */
    val nodeScanOperator = prepareNodeScanOperator("n", Seq.empty, Seq.empty)
    val projectColumn = Seq(
      (
        "n.lst",
        Property(
          Variable("n")(defaultPosition),
          PropertyKeyName("lst")(defaultPosition)
        )(defaultPosition)
      )
    )
    val projectOperator =
      ProjectOperator(nodeScanOperator, projectColumn, expressionEvaluator, ctx.expressionContext)
    val unwindOp = UnwindOperator(
      projectOperator,
      Seq("n.lst"),
      expressionEvaluator,
      ctx.expressionContext
    )
    val res = getOperatorAllOutputs(unwindOp).flatMap(batch => batch.batchData).flatten.toList
    Assert.assertEquals(
      List(
        LynxInteger(1),
        LynxInteger(2),
        LynxInteger(3),
        LynxNull,
        LynxList(List(1, 2, 3).map(LynxInteger(_))),
        LynxList(List(4, 5, 6).map(LynxInteger(_)))
      ),
      res
    )
  }
}
