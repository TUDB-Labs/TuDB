package org.grapheco.lynx.execution

import org.grapheco.lynx.ConstrainViolatedException
import org.grapheco.lynx.expression.pattern.{LynxNodePattern, LynxRelationshipPattern}
import org.grapheco.lynx.expression.{LynxStringLiteral, LynxVariable}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.{LynxInteger, LynxString}
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey, LynxRelationshipType}
import org.junit.{Assert, Test}
import org.opencypher.v9_0.expressions.{MapExpression, NodePattern, PropertyKeyName, RelTypeName, RelationshipPattern, SemanticDirection, StringLiteral, Variable}
import org.opencypher.v9_0.util.InputPosition

/**
  *@description:
  */
class DeleteOperatorTest extends BaseOperatorTest {
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
    Seq(LynxNodeLabel("Animal")),
    Map(LynxPropertyKey("name") -> LynxValue("Cat"), LynxPropertyKey("age") -> LynxInteger(20))
  )
  all_nodes.append(node1, node2, node3)

  @Test
  def testDeleteMultipleColumn(): Unit = {
    val smallTable =
      prepareNodeScanOperator(LynxVariable("animal", 0), Seq(LynxNodeLabel("Animal")), Seq.empty)
    val largeTable = prepareNodeScanOperator(
      LynxVariable("person", 0),
      Seq(LynxNodeLabel("Person")),
      Seq((LynxPropertyKey("name"), LynxStringLiteral("Alex")))
    )
    val joinOperator = JoinOperator(
      smallTable,
      largeTable,
      Seq.empty,
      expressionEvaluator,
      ctx.expressionContext
    )
    val selectOperator = SelectOperator(
      joinOperator,
      Seq(("animal", Option("animal")), ("person", Option("person"))),
      expressionEvaluator,
      ctx.expressionContext
    )
    val deleteOperator = DeleteOperator(
      selectOperator,
      model,
      Seq(Variable("animal")(defaultPosition), Variable("person")(defaultPosition)),
      false,
      expressionEvaluator,
      ctx.expressionContext
    )
    getOperatorAllOutputs(deleteOperator)
    model.commit()
    Assert.assertEquals(1, all_nodes.size)
    Assert.assertEquals(node2, all_nodes.head)
  }

  @Test
  def testDeleteNodeWithoutRelationship(): Unit = {
    val nodeScanOperator =
      prepareNodeScanOperator(LynxVariable("n", 0), Seq(LynxNodeLabel("Person")), Seq.empty)
    val selectOperator = SelectOperator(
      nodeScanOperator,
      Seq(("n", Option("n"))),
      expressionEvaluator,
      ctx.expressionContext
    )
    val deleteOperator = DeleteOperator(
      selectOperator,
      model,
      Seq(Variable("n")(defaultPosition)),
      false,
      expressionEvaluator,
      ctx.expressionContext
    )
    getOperatorAllOutputs(deleteOperator)
    model.commit()
    Assert.assertEquals(1, all_nodes.size)
    Assert.assertEquals(node3, all_nodes.head)
  }

  @Test(expected = classOf[ConstrainViolatedException])
  def testNotForceToDeleteNodeWithRelationship(): Unit = {
    val r1 = TestRelationship(
      TestId(1L),
      TestId(1L),
      TestId(2L),
      Option(LynxRelationshipType("KNOW")),
      Map.empty
    )
    all_rels.append(r1)

    val nodeScanOperator =
      prepareNodeScanOperator(LynxVariable("n", 0), Seq(LynxNodeLabel("Person")), Seq.empty)
    val selectOperator = SelectOperator(
      nodeScanOperator,
      Seq(("n", Option("n"))),
      expressionEvaluator,
      ctx.expressionContext
    )
    val deleteOperator = DeleteOperator(
      selectOperator,
      model,
      Seq(Variable("n")(defaultPosition)),
      false,
      expressionEvaluator,
      ctx.expressionContext
    )
    getOperatorAllOutputs(deleteOperator)
    model.commit()
  }

  @Test
  def testForceToDeleteNodeWithRelationship(): Unit = {
    val r1 = TestRelationship(
      TestId(1L),
      TestId(1L),
      TestId(2L),
      Option(LynxRelationshipType("KNOW")),
      Map.empty
    )
    all_rels.append(r1)

    val nodeScanOperator =
      prepareNodeScanOperator(LynxVariable("n", 0), Seq(LynxNodeLabel("Person")), Seq.empty)
    val selectOperator = SelectOperator(
      nodeScanOperator,
      Seq(("n", Option("n"))),
      expressionEvaluator,
      ctx.expressionContext
    )
    val deleteOperator = DeleteOperator(
      selectOperator,
      model,
      Seq(Variable("n")(defaultPosition)),
      true,
      expressionEvaluator,
      ctx.expressionContext
    )
    getOperatorAllOutputs(deleteOperator)
    model.commit()
    Assert.assertEquals(1, all_nodes.size)
    Assert.assertEquals(node3, all_nodes.head)
    Assert.assertEquals(0, all_rels.size)
  }

  @Test
  def testDeleteRelationship(): Unit = {
    val rPattern = LynxRelationshipPattern(
      LynxVariable("r", 1),
      Seq(LynxRelationshipType("KNOW")),
      1,
      1,
      None,
      SemanticDirection.OUTGOING
    )
    val leftPattern = LynxNodePattern(LynxVariable("n", 0), Seq.empty, None)
    val rightPattern = LynxNodePattern(LynxVariable("m", 2), Seq.empty, None)
    val r1 = TestRelationship(
      TestId(1L),
      TestId(1L),
      TestId(2L),
      Option(LynxRelationshipType("KNOW")),
      Map.empty
    )
    all_rels.append(r1)
    val pathScanOperator = PathScanOperator(
      rPattern,
      leftPattern,
      rightPattern,
      model,
      expressionEvaluator,
      ctx.expressionContext
    )
    val selectOperator = SelectOperator(
      pathScanOperator,
      Seq(("r", Option("r"))),
      expressionEvaluator,
      ctx.expressionContext
    )
    val deleteOperator = DeleteOperator(
      selectOperator,
      model,
      Seq(Variable("r")(defaultPosition)),
      false,
      expressionEvaluator,
      ctx.expressionContext
    )
    getOperatorAllOutputs(deleteOperator)
    model.commit()
    Assert.assertEquals(0, all_rels.size)
  }

}
