package org.grapheco.lynx.operator

import org.apache.commons.collections4.CollectionUtils
import org.grapheco.lynx.operator.join.JoinType
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey}
import org.junit.{Assert, Test}
import org.opencypher.v9_0.expressions.{Equals, Property, PropertyKeyName, Variable}

import scala.collection.JavaConverters._

class JoinOperatorTest extends BaseOperatorTest {
  val node1 = TestNode(
    TestId(1L),
    Seq(LynxNodeLabel("Person")),
    Map(
      LynxPropertyKey("name") -> LynxValue("Alex"),
      LynxPropertyKey("city") -> LynxValue("NewYork")
    )
  )
  val node2 = TestNode(
    TestId(2L),
    Seq(LynxNodeLabel("Person")),
    Map(
      LynxPropertyKey("name") -> LynxValue("Bob"),
      LynxPropertyKey("city") -> LynxValue("Beijing")
    )
  )
  val node3 = TestNode(
    TestId(3L),
    Seq(LynxNodeLabel("Person")),
    Map(
      LynxPropertyKey("name") -> LynxValue("Cat"),
      LynxPropertyKey("city") -> LynxValue("Chengdu")
    )
  )
  val node4 = TestNode(
    TestId(4L),
    Seq(LynxNodeLabel("City")),
    Map(LynxPropertyKey("name") -> LynxValue("Beijing"))
  )
  val node5 = TestNode(
    TestId(5L),
    Seq(LynxNodeLabel("City")),
    Map(LynxPropertyKey("name") -> LynxValue("Chengdu"))
  )
  all_nodes.append(node1, node2, node3, node4, node5)

  @Test
  def testCartesianProduct(): Unit = {
    val smallTable = prepareNodeScanOperator("city", Seq("City"), Seq.empty)
    val largeTable = prepareNodeScanOperator("person", Seq("Person"), Seq.empty)
    val joinOperator = JoinOperator(
      smallTable,
      largeTable,
      JoinType.DEFAULT,
      Seq.empty,
      expressionEvaluator,
      ctx.expressionContext
    )
    val res = getOperatorAllOutputs(joinOperator)
      .map(f => f.batchData.map(f => f.asJava).asJava)
      .toList
      .asJava
    println(res)
    Assert.assertTrue(
      CollectionUtils.isEqualCollection(
        List(
          List(
            List(node4, node1).asJava,
            List(node5, node1).asJava
          ).asJava,
          List(
            List(node4, node2).asJava,
            List(node5, node2).asJava
          ).asJava,
          List(
            List(node4, node3).asJava,
            List(node5, node3).asJava
          ).asJava
        ).asJava,
        res
      )
    )
  }

  @Test
  def testValueHashJoin(): Unit = {
    val filterExpr = Equals(
      Property(Variable("person")(defaultPosition), PropertyKeyName("city")(defaultPosition))(
        defaultPosition
      ),
      Property(Variable("city")(defaultPosition), PropertyKeyName("name")(defaultPosition))(
        defaultPosition
      )
    )(defaultPosition)

    val smallTable = prepareNodeScanOperator("city", Seq("City"), Seq.empty)
    val largeTable = prepareNodeScanOperator("person", Seq("Person"), Seq.empty)
    val joinOperator = JoinOperator(
      smallTable,
      largeTable,
      JoinType.DEFAULT,
      Seq(filterExpr),
      expressionEvaluator,
      ctx.expressionContext
    )
    val res = getOperatorAllOutputs(joinOperator)
      .map(f => f.batchData.map(f => f.asJava).asJava)
      .toList
      .asJava
    Assert.assertTrue(
      CollectionUtils.isEqualCollection(
        List(
          List(List(node4, node2).asJava).asJava,
          List(List(node5, node3).asJava).asJava
        ).asJava,
        res
      )
    )
  }
}
