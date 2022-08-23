package org.grapheco.lynx.operator

import org.apache.commons.collections4.CollectionUtils
import org.grapheco.lynx.CreateNode
import org.grapheco.lynx.types.property.LynxString
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey}
import org.junit.{Assert, Test}
import org.opencypher.v9_0.expressions.{LabelName, MapExpression, PropertyKeyName, StringLiteral}
import org.opencypher.v9_0.util.symbols.CTNode

import scala.collection.JavaConverters._

/**
  *@description:
  */
class CreateOperatorTest extends BaseOperatorTest {
  val node1 = TestNode(
    TestId(1L),
    Seq(LynxNodeLabel("Person")),
    Map(LynxPropertyKey("name") -> LynxString("AAA"))
  )
  val node2 = TestNode(
    TestId(2L),
    Seq(LynxNodeLabel("Person")),
    Map(LynxPropertyKey("name") -> LynxString("BBB"))
  )
  @Test
  def testCreateSingleNode() {
    val propExpr = MapExpression(
      Seq((PropertyKeyName("name")(defaultPosition), StringLiteral("AAA")(defaultPosition)))
    )(defaultPosition)
    val operator = CreateOperator(
      None,
      Seq(("n", CTNode)),
      Seq(CreateNode("n", Seq(LabelName("Person")(defaultPosition)), Option(propExpr))),
      model,
      expressionEvaluator,
      ctx.expressionContext
    )
    val res = getOperatorAllOutputs(operator)
    model.commit()

    Assert.assertEquals(node1, res.head.batchData.head.head)
    Assert.assertEquals(1, all_nodes.length)
  }
  @Test
  def testCreateMultipleNodes(): Unit = {
    val propExpr1 = MapExpression(
      Seq((PropertyKeyName("name")(defaultPosition), StringLiteral("AAA")(defaultPosition)))
    )(defaultPosition)
    val operator1 = CreateOperator(
      None,
      Seq(("n", CTNode)),
      Seq(CreateNode("n", Seq(LabelName("Person")(defaultPosition)), Option(propExpr1))),
      model,
      expressionEvaluator,
      ctx.expressionContext
    )
    val propExpr2 = MapExpression(
      Seq((PropertyKeyName("name")(defaultPosition), StringLiteral("BBB")(defaultPosition)))
    )(defaultPosition)
    val operator2 = CreateOperator(
      Option(operator1),
      Seq(("m", CTNode)),
      Seq(CreateNode("m", Seq(LabelName("Person")(defaultPosition)), Option(propExpr2))),
      model,
      expressionEvaluator,
      ctx.expressionContext
    )
    val res = getOperatorAllOutputs(operator2).flatMap(b => b.batchData.flatten).toList.asJava
    model.commit()
    Assert.assertTrue(
      CollectionUtils.isEqualCollection(
        List(node1, node2).asJava,
        res
      )
    )
    Assert.assertEquals(2, all_nodes.length)
  }

  @Test
  def testCreateNodeFromInProperty(): Unit = {
    /*
       match (n: Person)
       create (m:City{name:n.name})
       return m
     */
    all_nodes.append(node1)

  }
  @Test
  def testCreateSingleRelationship(): Unit = {}
  @Test
  def testCreateMultipleRelationship(): Unit = {}
  @Test
  def testCreateRelationshipFromInProperties(): Unit = {}
}
