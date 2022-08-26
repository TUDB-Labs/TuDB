package org.grapheco.lynx.operator

import org.apache.commons.collections4.CollectionUtils
import org.grapheco.lynx.{CreateNode, CreateRelationship}
import org.grapheco.lynx.types.property.LynxString
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey, LynxRelationshipType}
import org.junit.{Assert, Test}
import org.opencypher.v9_0.expressions.{LabelName, MapExpression, Property, PropertyKeyName, RelTypeName, StringLiteral, Variable}
import org.opencypher.v9_0.util.symbols.{CTNode, CTRelationship}

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
  val r1 = TestRelationship(
    TestId(1L),
    TestId(1L),
    TestId(2L),
    Option(LynxRelationshipType("KNOW")),
    Map(LynxPropertyKey("name") -> LynxString("R"))
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
    val expr = MapExpression(
      Seq(
        (
          PropertyKeyName("name")(defaultPosition),
          Property(Variable("n")(defaultPosition), PropertyKeyName("name")(defaultPosition))(
            defaultPosition
          )
        )
      )
    )(defaultPosition)
    all_nodes.append(node1)
    _nodeId = 1
    val nodeScanOperator = prepareNodeScanOperator("n", Seq("Person"), Seq.empty)
    val createOperator = CreateOperator(
      Option(nodeScanOperator),
      Seq(("n2", CTNode)),
      Seq(CreateNode("n2", Seq(LabelName("NEW_PERSON")(defaultPosition)), Option(expr))),
      model,
      expressionEvaluator,
      ctx.expressionContext
    )
    getOperatorAllOutputs(createOperator)
    model.commit()

    val createdNode = TestNode(
      TestId(2L),
      Seq(LynxNodeLabel("NEW_PERSON")),
      Map(LynxPropertyKey("name") -> LynxString("AAA"))
    )

    Assert.assertTrue(
      CollectionUtils.isEqualCollection(
        List(node1, createdNode).asJava,
        all_nodes.toList.asJava
      )
    )
  }

  @Test
  def testCreateSingleRelationship(): Unit = {
    /*
        match (n: Person) where n.name = 'A'
        match (m: Person) where m.name = 'B'
        create (n)-[r:KNOW{name:'AAA'}]->(m)
     */
    val nodeScanOperator1 = prepareNodeScanOperator(
      "n",
      Seq("Person"),
      Seq((PropertyKeyName("name")(defaultPosition), StringLiteral("AAA")(defaultPosition)))
    )
    val nodeScanOperator2 = prepareNodeScanOperator(
      "m",
      Seq("Person"),
      Seq((PropertyKeyName("name")(defaultPosition), StringLiteral("BBB")(defaultPosition)))
    )
    val joinOperator = ""
    // TODO: After merge JoinOperator.
  }
  @Test
  def testCreateNodeAndRelationship(): Unit = {
    /*
        create (n:Person{name:'AAA'})-[r:KNOW{name:'R'}]->(m: Person{name: 'BBB'})
     */
    val nPropExpr = MapExpression(
      Seq((PropertyKeyName("name")(defaultPosition), StringLiteral("AAA")(defaultPosition)))
    )(defaultPosition)
    val mPropExpr = MapExpression(
      Seq((PropertyKeyName("name")(defaultPosition), StringLiteral("BBB")(defaultPosition)))
    )(defaultPosition)
    val rPropExpr = MapExpression(
      Seq((PropertyKeyName("name")(defaultPosition), StringLiteral("R")(defaultPosition)))
    )(defaultPosition)
    val operator = CreateOperator(
      None,
      Seq(("n", CTNode), ("m", CTNode), ("r", CTRelationship)),
      Seq(
        CreateNode("n", Seq(LabelName("Person")(defaultPosition)), Option(nPropExpr)),
        CreateNode("m", Seq(LabelName("Person")(defaultPosition)), Option(mPropExpr)),
        CreateRelationship(
          "r",
          Seq(RelTypeName("KNOW")(defaultPosition)),
          Option(rPropExpr),
          "n",
          "m"
        )
      ),
      model,
      expressionEvaluator,
      ctx.expressionContext
    )
    val res = getOperatorAllOutputs(operator).flatMap(r => r.batchData.flatten)
    model.commit()
    Assert.assertTrue(
      CollectionUtils.isEqualCollection(
        List(node1, node2, r1).asJava,
        res.toList.asJava
      )
    )
  }

}
