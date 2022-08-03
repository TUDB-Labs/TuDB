package org.grapheco.lynx.operator

import org.apache.commons.collections4.CollectionUtils
import org.grapheco.lynx.RowBatch
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey, LynxRelationshipType}
import org.junit.{Assert, Test}
import org.opencypher.v9_0.expressions.{Expression, LabelName, LogicalVariable, MapExpression, NodePattern, PropertyKeyName, Range, RelTypeName, RelationshipPattern, SemanticDirection, SignedDecimalIntegerLiteral, StringLiteral, Variable}
import org.opencypher.v9_0.util.InputPosition

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

/**
  *@author:John117
  *@createDate:2022/8/2
  *@description:
  */
class RelationshipScanOperatorTest extends BaseOperatorTest {
  val node1 = TestNode(
    TestId(1L),
    Seq(LynxNodeLabel("Person")),
    Map(LynxPropertyKey("name") -> LynxValue("Alex"))
  )
  val node2 = TestNode(
    TestId(2L),
    Seq(LynxNodeLabel("Person")),
    Map(LynxPropertyKey("name") -> LynxValue("Bob"))
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

  val rel1 = TestRelationship(
    TestId(1L),
    TestId(1L),
    TestId(2L),
    Option(LynxRelationshipType("KNOW")),
    Map(LynxPropertyKey("year") -> LynxInteger(2022))
  )
  val rel2 = TestRelationship(
    TestId(1L),
    TestId(2L),
    TestId(3L),
    Option(LynxRelationshipType("KNOW")),
    Map(LynxPropertyKey("year") -> LynxInteger(2021))
  )

  all_nodes.append(node1, node2, node3, node4)
  all_rels.append(rel1, rel2)

  @Test
  def testRelationshipScan(): Unit = {
    val leftPattern = NodePattern(None, Seq.empty, None)(InputPosition(0, 0, 0))
    val rightPattern = NodePattern(None, Seq.empty, None)(InputPosition(0, 0, 0))
    val relPattern = RelationshipPattern(
      None,
      Seq(RelTypeName("KNOW")(InputPosition(0, 0, 0))),
      None,
      None,
      SemanticDirection.OUTGOING
    )(InputPosition(0, 0, 0))

    val relationshipScanOperator = RelationshipScanOperator(
      relPattern,
      leftPattern,
      rightPattern,
      model,
      expressionEvaluator,
      ctx.expressionContext
    )
    val resultData = ArrayBuffer[RowBatch]()
    relationshipScanOperator.open()
    var data = relationshipScanOperator.getNext()
    while (data.batchData.nonEmpty) {
      resultData.append(data)
      data = relationshipScanOperator.getNext()
    }
    relationshipScanOperator.close()

    val target = List(TestPathTriple(node1, rel1, node2), TestPathTriple(node2, rel2, node3)).asJava
    val searched = resultData
      .map(f => f.batchData.flatten)
      .map(f =>
        TestPathTriple(
          f.head.asInstanceOf[TestNode],
          f(1).asInstanceOf[TestRelationship],
          f.last.asInstanceOf[TestNode]
        )
      )
      .toList
      .asJava
    Assert.assertTrue(CollectionUtils.isEqualCollection(target, searched))
  }

  @Test
  def testRelationshipScanWithRelationFilter(): Unit = {
    val leftPattern = NodePattern(None, Seq.empty, None)(InputPosition(0, 0, 0))
    val rightPattern = NodePattern(None, Seq.empty, None)(InputPosition(0, 0, 0))
    val relPattern = RelationshipPattern(
      None,
      Seq(RelTypeName("KNOW")(InputPosition(0, 0, 0))),
      None,
      Option(
        MapExpression(
          Seq(
            (
              PropertyKeyName("year")(InputPosition(0, 0, 0)),
              SignedDecimalIntegerLiteral("2021")(InputPosition(0, 0, 0))
            )
          )
        )(InputPosition(0, 0, 0))
      ),
      SemanticDirection.OUTGOING
    )(InputPosition(0, 0, 0))

    val relationshipScanOperator = RelationshipScanOperator(
      relPattern,
      leftPattern,
      rightPattern,
      model,
      expressionEvaluator,
      ctx.expressionContext
    )
    val resultData = ArrayBuffer[RowBatch]()
    relationshipScanOperator.open()
    var data = relationshipScanOperator.getNext()
    while (data.batchData.nonEmpty) {
      resultData.append(data)
      data = relationshipScanOperator.getNext()
    }
    relationshipScanOperator.close()

    val target = List(TestPathTriple(node2, rel2, node3)).asJava
    val searched = resultData
      .map(f => f.batchData.flatten)
      .map(f =>
        TestPathTriple(
          f.head.asInstanceOf[TestNode],
          f(1).asInstanceOf[TestRelationship],
          f.last.asInstanceOf[TestNode]
        )
      )
      .toList
      .asJava
    Assert.assertTrue(CollectionUtils.isEqualCollection(target, searched))
  }
}
