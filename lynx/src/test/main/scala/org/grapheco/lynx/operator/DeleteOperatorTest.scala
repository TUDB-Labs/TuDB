package org.grapheco.lynx.operator

import org.grapheco.lynx.ConstrainViolatedException
import org.grapheco.lynx.operator.utils.OperatorUtils
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey, LynxRelationshipType}
import org.junit.{Assert, Test}
import org.opencypher.v9_0.ast.Delete
import org.opencypher.v9_0.expressions.{NodePattern, RelTypeName, RelationshipPattern, SemanticDirection, Variable}

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
  def testDeleteNodeWithoutRelationship(): Unit = {
    val nodeScanOperator = prepareNodeScanOperator("n", Seq("Person"), Seq.empty)
    val selectOperator = SelectOperator(
      Seq(("n", Option("n"))),
      nodeScanOperator,
      expressionEvaluator,
      ctx.expressionContext
    )
    val deleteOperator = DeleteOperator(
      selectOperator,
      model,
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

    val nodeScanOperator = prepareNodeScanOperator("n", Seq("Person"), Seq.empty)
    val selectOperator = SelectOperator(
      Seq(("n", Option("n"))),
      nodeScanOperator,
      expressionEvaluator,
      ctx.expressionContext
    )
    val deleteOperator = DeleteOperator(
      selectOperator,
      model,
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

    val nodeScanOperator = prepareNodeScanOperator("n", Seq("Person"), Seq.empty)
    val selectOperator = SelectOperator(
      Seq(("n", Option("n"))),
      nodeScanOperator,
      expressionEvaluator,
      ctx.expressionContext
    )
    val deleteOperator = DeleteOperator(
      selectOperator,
      model,
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
    val rPattern = RelationshipPattern(
      Option(Variable("r")(defaultPosition)),
      Seq(RelTypeName("KNOW")(defaultPosition)),
      None,
      None,
      SemanticDirection.OUTGOING
    )(defaultPosition)
    val leftPattern = NodePattern(None, Seq.empty, None)(defaultPosition)
    val rightPattern = NodePattern(None, Seq.empty, None)(defaultPosition)
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
      Seq(("r", Option("r"))),
      pathScanOperator,
      expressionEvaluator,
      ctx.expressionContext
    )
    val deleteOperator = DeleteOperator(
      selectOperator,
      model,
      false,
      expressionEvaluator,
      ctx.expressionContext
    )
    getOperatorAllOutputs(deleteOperator)
    model.commit()
    Assert.assertEquals(0, all_rels.size)
  }

}
