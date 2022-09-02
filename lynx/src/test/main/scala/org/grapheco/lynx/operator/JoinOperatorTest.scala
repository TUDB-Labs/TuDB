package org.grapheco.lynx.operator

import org.apache.commons.collections4.CollectionUtils
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey, LynxRelationshipType}
import org.junit.{Assert, Test}
import org.opencypher.v9_0.expressions.{Equals, NodePattern, Property, PropertyKeyName, RelTypeName, RelationshipPattern, SemanticDirection, Variable}

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

  val rel1 = TestRelationship(
    TestId(1L),
    TestId(3L),
    TestId(5L),
    Option(LynxRelationshipType("LiveIn")),
    Map.empty
  )
  val rel2 = TestRelationship(
    TestId(2L),
    TestId(3L),
    TestId(1L),
    Option(LynxRelationshipType("KNOW")),
    Map.empty
  )

  all_nodes.append(node1, node2, node3, node4, node5)
  all_rels.append(rel1, rel2)

  @Test
  def testCartesianProduct(): Unit = {
    val smallTable = prepareNodeScanOperator("city", Seq("City"), Seq.empty)
    val largeTable = prepareNodeScanOperator("person", Seq("Person"), Seq.empty)
    val joinOperator = JoinOperator(
      smallTable,
      largeTable,
      Seq.empty,
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
  def testFilterJoin(): Unit = {
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

  @Test
  def testJoinWithSameCols(): Unit = {
    /*
        match (n: Person)-[r1:LiveIn]->(c:City)
        match (n)-[r2:KNOW]->(m)
        return m
     */
    val leftPattern1 =
      NodePattern(Option(Variable("n")(defaultPosition)), Seq.empty, None)(defaultPosition)
    val rightPattern1 =
      NodePattern(Option(Variable("c")(defaultPosition)), Seq.empty, None)(defaultPosition)
    val relPattern1 = RelationshipPattern(
      None,
      Seq(RelTypeName("LiveIn")(defaultPosition)),
      None,
      None,
      SemanticDirection.OUTGOING
    )(defaultPosition)
    val relationshipScanOperator1 = PathScanOperator(
      relPattern1,
      leftPattern1,
      rightPattern1,
      model,
      expressionEvaluator,
      ctx.expressionContext
    )

    val leftPattern2 =
      NodePattern(Option(Variable("n")(defaultPosition)), Seq.empty, None)(defaultPosition)
    val rightPattern2 =
      NodePattern(Option(Variable("m")(defaultPosition)), Seq.empty, None)(defaultPosition)
    val relPattern2 = RelationshipPattern(
      None,
      Seq(RelTypeName("KNOW")(defaultPosition)),
      None,
      None,
      SemanticDirection.OUTGOING
    )(defaultPosition)
    val relationshipScanOperator2 = PathScanOperator(
      relPattern2,
      leftPattern2,
      rightPattern2,
      model,
      expressionEvaluator,
      ctx.expressionContext
    )

    val joinOperator = JoinOperator(
      relationshipScanOperator1,
      relationshipScanOperator2,
      Seq.empty,
      expressionEvaluator,
      ctx.expressionContext
    )
    val res = getOperatorAllOutputs(joinOperator).flatMap(f => f.batchData).flatten
    Assert.assertEquals(
      Array(node3, rel1, node5, rel2, node1).toList,
      res.toList
    )
  }
}
