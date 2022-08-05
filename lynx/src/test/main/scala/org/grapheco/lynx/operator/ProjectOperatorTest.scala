package org.grapheco.lynx.operator

import org.apache.commons.collections4.CollectionUtils
import org.grapheco.lynx.RowBatch
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey}
import org.junit.{Assert, Test}
import org.opencypher.v9_0.expressions.{Property, PropertyKeyName, Variable}
import scala.collection.JavaConverters._

/**
  *@author:John117
  *@createDate:2022/8/3
  *@description:
  */
class ProjectOperatorTest extends BaseOperatorTest {
  val node1 = TestNode(
    TestId(1L),
    Seq(LynxNodeLabel("Person")),
    Map(LynxPropertyKey("name") -> LynxValue("Alex"), LynxPropertyKey("age") -> LynxInteger(10))
  )
  val node2 = TestNode(
    TestId(2L),
    Seq(LynxNodeLabel("Person")),
    Map(LynxPropertyKey("name") -> LynxValue("Bob"))
  )
  val node3 = TestNode(
    TestId(3L),
    Seq(LynxNodeLabel("Person")),
    Map(LynxPropertyKey("name") -> LynxValue("Cat"), LynxPropertyKey("age") -> LynxInteger(20))
  )
  all_nodes.append(node1, node2, node3)

  @Test
  def testProjectSelectedSingleProperty(): Unit = {
    val inOperator = prepareNodeScanOperator("n", Seq("Person"), Seq.empty)
    val projectColumn = Seq(
      (
        "n.name",
        Property(
          Variable("n")(defaultPosition),
          PropertyKeyName("name")(defaultPosition)
        )(defaultPosition)
      )
    )
    val projectOperator =
      ProjectOperator(
        inOperator,
        Seq(("n.name", Option("n.name"))),
        projectColumn,
        model,
        expressionEvaluator,
        ctx.expressionContext
      )

    val result = getOperatorAllOutputs(projectOperator)

    Assert.assertTrue(
      CollectionUtils.isEqualCollection(
        List("Alex", "Bob", "Cat").asJava,
        result.flatMap(f => f.batchData.flatten.map(f => f.value)).toList.asJava
      )
    )
  }

  @Test
  def testProjectSelectedMultipleProperty(): Unit = {
    val inOperator = prepareNodeScanOperator("n", Seq("Person"), Seq.empty)
    val projectColumn = Seq(
      (
        "n.name",
        Property(
          Variable("n")(defaultPosition),
          PropertyKeyName("name")(defaultPosition)
        )(defaultPosition)
      ),
      (
        "n.age",
        Property(
          Variable("n")(defaultPosition),
          PropertyKeyName("age")(defaultPosition)
        )(defaultPosition)
      )
    )
    val projectOperator =
      ProjectOperator(
        inOperator,
        Seq(("n.name", Option("n.name")), ("n.age", Option("n.age"))),
        projectColumn,
        model,
        expressionEvaluator,
        ctx.expressionContext
      )

    val result = getOperatorAllOutputs(projectOperator)

    Assert.assertTrue(
      CollectionUtils.isEqualCollection(
        List(List("Alex", 10L).asJava, List("Bob", null).asJava, List("Cat", 20L).asJava).asJava,
        seqAsJavaList(
          result.flatMap(batch => batch.batchData.map(f => f.map(ff => ff.value).asJava))
        )
      )
    )
  }

  @Test
  def testNoProjectSelect(): Unit = {
    val inOperator = prepareNodeScanOperator("n", Seq("Person"), Seq.empty)
    val projectOperator =
      ProjectOperator(
        inOperator,
        Seq(("n", Option("n"))),
        Seq.empty,
        model,
        expressionEvaluator,
        ctx.expressionContext
      )
    val result = getOperatorAllOutputs(projectOperator)
    Assert.assertTrue(
      CollectionUtils.isEqualCollection(
        List(node1, node2, node3).asJava,
        seqAsJavaList(result.flatMap(batch => batch.batchData.flatten))
      )
    )
  }
}
