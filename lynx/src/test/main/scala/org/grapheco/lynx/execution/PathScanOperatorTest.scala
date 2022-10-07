package org.grapheco.lynx.execution

import org.apache.commons.collections4.CollectionUtils
import org.grapheco.lynx.expression.{LynxMapExpression, LynxSignedDecimalIntegerLiteral, LynxStringLiteral, LynxVariable}
import org.grapheco.lynx.expression.pattern.{LynxNodePattern, LynxRelationshipPattern}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey, LynxRelationshipType}
import org.junit.{Assert, Test}
import org.opencypher.v9_0.expressions.SemanticDirection

import scala.collection.JavaConverters._

class PathScanOperatorTest extends BaseOperatorTest {
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

  def prepareOutgoingHopsData(): Unit = {
    all_nodes.clear()
    all_rels.clear()
    val extraNode = TestNode(
      TestId(8L),
      Seq(LynxNodeLabel("Person")),
      Map(
        LynxPropertyKey("name") -> LynxValue("A"),
        LynxPropertyKey("age") -> LynxValue(233)
      )
    )
    val n1 = TestNode(
      TestId(1L),
      Seq(LynxNodeLabel("Person")),
      Map(LynxPropertyKey("name") -> LynxValue("A"))
    )
    val n2 = TestNode(
      TestId(2L),
      Seq(LynxNodeLabel("Person")),
      Map(LynxPropertyKey("name") -> LynxValue("B"))
    )
    val n3 = TestNode(
      TestId(3L),
      Seq(LynxNodeLabel("Person")),
      Map(LynxPropertyKey("name") -> LynxValue("C"))
    )
    val n4 = TestNode(
      TestId(4L),
      Seq(LynxNodeLabel("Person")),
      Map(LynxPropertyKey("name") -> LynxValue("D"))
    )
    val n5 = TestNode(
      TestId(5L),
      Seq(LynxNodeLabel("Person")),
      Map(LynxPropertyKey("name") -> LynxValue("E"))
    )
    val n6 = TestNode(
      TestId(6L),
      Seq(LynxNodeLabel("Person")),
      Map(LynxPropertyKey("name") -> LynxValue("F"))
    )
    val n7 = TestNode(
      TestId(7L),
      Seq(LynxNodeLabel("Person")),
      Map(LynxPropertyKey("name") -> LynxValue("G"))
    )
    val r1 = TestRelationship(
      TestId(1L),
      TestId(1L),
      TestId(2L),
      Option(LynxRelationshipType("KNOW")),
      Map.empty
    )
    val r2 = TestRelationship(
      TestId(2L),
      TestId(1L),
      TestId(3L),
      Option(LynxRelationshipType("KNOW")),
      Map.empty
    )
    val r3 = TestRelationship(
      TestId(3L),
      TestId(1L),
      TestId(4L),
      Option(LynxRelationshipType("KNOW")),
      Map.empty
    )
    val r4 = TestRelationship(
      TestId(4L),
      TestId(2L),
      TestId(5L),
      Option(LynxRelationshipType("KNOW")),
      Map.empty
    )
    val r5 = TestRelationship(
      TestId(5L),
      TestId(2L),
      TestId(6L),
      Option(LynxRelationshipType("KNOW")),
      Map.empty
    )
    val r6 = TestRelationship(
      TestId(6L),
      TestId(6L),
      TestId(7L),
      Option(LynxRelationshipType("KNOW")),
      Map.empty
    )
    all_nodes.append(n1, n2, n3, n4, n5, n6, n7, extraNode)
    all_rels.append(r1, r2, r3, r4, r5, r6)
  }

  def prepareBothHopsData(): Unit = {
    all_nodes.clear()
    all_rels.clear()

    val n1 = TestNode(
      TestId(1L),
      Seq(LynxNodeLabel("Person")),
      Map(LynxPropertyKey("name") -> LynxValue("A"))
    )
    val n2 = TestNode(
      TestId(2L),
      Seq(LynxNodeLabel("Person")),
      Map(LynxPropertyKey("name") -> LynxValue("B"))
    )
    val n3 = TestNode(
      TestId(3L),
      Seq(LynxNodeLabel("Person")),
      Map(LynxPropertyKey("name") -> LynxValue("C"))
    )
    val n4 = TestNode(
      TestId(4L),
      Seq(LynxNodeLabel("Person")),
      Map(LynxPropertyKey("name") -> LynxValue("D"))
    )

    val r1 = TestRelationship(
      TestId(1L),
      TestId(1L),
      TestId(2L),
      Option(LynxRelationshipType("KNOW")),
      Map.empty
    )
    val r2 = TestRelationship(
      TestId(2L),
      TestId(1L),
      TestId(3L),
      Option(LynxRelationshipType("KNOW")),
      Map.empty
    )
    val r3 = TestRelationship(
      TestId(3L),
      TestId(4L),
      TestId(3L),
      Option(LynxRelationshipType("KNOW")),
      Map.empty
    )

    all_nodes.append(n1, n2, n3, n4)
    all_rels.append(r1, r2, r3)
  }
  @Test
  def testRelationshipScan(): Unit = {
    val leftPattern = LynxNodePattern(LynxVariable("n", 0), Seq.empty, None)
    val rightPattern = LynxNodePattern(LynxVariable("m", 2), Seq.empty, None)
    val relPattern = LynxRelationshipPattern(
      LynxVariable("r", 1),
      Seq(LynxRelationshipType("KNOW")),
      1,
      1,
      None,
      SemanticDirection.OUTGOING
    )

    val relationshipScanOperator = PathScanOperator(
      relPattern,
      leftPattern,
      rightPattern,
      model,
      expressionEvaluator,
      ctx.expressionContext
    )
    val resultData = getOperatorAllOutputs(relationshipScanOperator)

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
    val leftPattern = LynxNodePattern(LynxVariable("n", 0), Seq.empty, None)
    val rightPattern = LynxNodePattern(LynxVariable("m", 2), Seq.empty, None)
    val relPattern = LynxRelationshipPattern(
      LynxVariable("r", 1),
      Seq(LynxRelationshipType("KNOW")),
      1,
      1,
      Option(
        LynxMapExpression(
          Seq(
            (
              LynxPropertyKey("year"),
              LynxSignedDecimalIntegerLiteral("2021")
            )
          )
        )
      ),
      SemanticDirection.OUTGOING
    )

    val relationshipScanOperator = PathScanOperator(
      relPattern,
      leftPattern,
      rightPattern,
      model,
      expressionEvaluator,
      ctx.expressionContext
    )
    val resultData = getOperatorAllOutputs(relationshipScanOperator)

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

  @Test
  def testOutgoingHopSearch(): Unit = {
    prepareOutgoingHopsData()
    val leftPattern = LynxNodePattern(LynxVariable("n", 0), Seq.empty, None)
    val rightPattern = LynxNodePattern(LynxVariable("m", 2), Seq.empty, None)
    val relPattern1 = LynxRelationshipPattern(
      LynxVariable("r", 1),
      Seq(LynxRelationshipType("KNOW")),
      0,
      2,
      None,
      SemanticDirection.OUTGOING
    )

    val relationshipScanOperator = PathScanOperator(
      relPattern1,
      leftPattern,
      rightPattern,
      model,
      expressionEvaluator,
      ctx.expressionContext
    )
    val resultData =
      getOperatorAllOutputs(relationshipScanOperator).flatMap(inputBatch => inputBatch.batchData)

    // 0 hop: 8
    // 1 hop: 6
    // 2 hop: 3
    Assert.assertEquals(8 + 6 + 3, resultData.length)

    val relPattern2 = LynxRelationshipPattern(
      LynxVariable("r", 1),
      Seq(LynxRelationshipType("KNOW")),
      0,
      1000,
      None,
      SemanticDirection.OUTGOING
    )
    val relationshipScanOperator2 = PathScanOperator(
      relPattern2,
      leftPattern,
      rightPattern,
      model,
      expressionEvaluator,
      ctx.expressionContext
    )

    val resultData2 =
      getOperatorAllOutputs(relationshipScanOperator2).flatMap(inputBatch => inputBatch.batchData)

    // 0 hop: 7
    // 1 hop: 6
    // 2 hop: 3
    // 3 hop: 1
    Assert.assertEquals(8 + 6 + 3 + 1, resultData2.length)

    val relPattern3 = LynxRelationshipPattern(
      LynxVariable("r", 1),
      Seq(LynxRelationshipType("KNOW")),
      2,
      3,
      None,
      SemanticDirection.OUTGOING
    )
    val relationshipScanOperator3 = PathScanOperator(
      relPattern3,
      leftPattern,
      rightPattern,
      model,
      expressionEvaluator,
      ctx.expressionContext
    )

    val resultData3 =
      getOperatorAllOutputs(relationshipScanOperator3).flatMap(inputBatch => inputBatch.batchData)

    // 2 hop: 3
    // 3 hop: 1
    Assert.assertEquals(3 + 1, resultData3.length)
  }
  @Test
  def testIncomingHopSearch(): Unit = {
    prepareOutgoingHopsData()
    val leftPattern = LynxNodePattern(LynxVariable("n", 0), Seq.empty, None)
    val rightPattern = LynxNodePattern(LynxVariable("m", 2), Seq.empty, None)
    val relPattern1 = LynxRelationshipPattern(
      LynxVariable("r", 1),
      Seq(LynxRelationshipType("KNOW")),
      0,
      2,
      None,
      SemanticDirection.INCOMING
    )

    val relationshipScanOperator = PathScanOperator(
      relPattern1,
      leftPattern,
      rightPattern,
      model,
      expressionEvaluator,
      ctx.expressionContext
    )
    val resultData =
      getOperatorAllOutputs(relationshipScanOperator).flatMap(inputBatch => inputBatch.batchData)

    // 0 hop: 8
    // 1 hop: 6
    // 2 hop: 3
    Assert.assertEquals(8 + 6 + 3, resultData.length)

    val relPattern2 = LynxRelationshipPattern(
      LynxVariable("r", 1),
      Seq(LynxRelationshipType("KNOW")),
      0,
      1000,
      None,
      SemanticDirection.INCOMING
    )

    val relationshipScanOperator2 = PathScanOperator(
      relPattern2,
      leftPattern,
      rightPattern,
      model,
      expressionEvaluator,
      ctx.expressionContext
    )

    val resultData2 =
      getOperatorAllOutputs(relationshipScanOperator2).flatMap(inputBatch => inputBatch.batchData)

    // 0 hop: 7
    // 1 hop: 6
    // 2 hop: 3
    // 3 hop: 1
    Assert.assertEquals(8 + 6 + 3 + 1, resultData2.length)

    val relPattern3 = LynxRelationshipPattern(
      LynxVariable("r", 1),
      Seq(LynxRelationshipType("KNOW")),
      2,
      3,
      None,
      SemanticDirection.INCOMING
    )
    val relationshipScanOperator3 = PathScanOperator(
      relPattern3,
      leftPattern,
      rightPattern,
      model,
      expressionEvaluator,
      ctx.expressionContext
    )

    val resultData3 =
      getOperatorAllOutputs(relationshipScanOperator3).flatMap(inputBatch => inputBatch.batchData)

    // 2 hop: 3
    // 3 hop: 1
    Assert.assertEquals(3 + 1, resultData3.length)
  }

  @Test
  def testOutgoingIncomingHopSearchWithSpecifiedNodes(): Unit = {
    prepareOutgoingHopsData()

    val leftPattern = LynxNodePattern(
      LynxVariable("n", 0),
      Seq.empty,
      Option(
        LynxMapExpression(
          Seq(
            (LynxPropertyKey("name"), LynxStringLiteral("A"))
          )
        )
      )
    )
    val rightPattern = LynxNodePattern(
      LynxVariable("m", 2),
      Seq.empty,
      Option(
        LynxMapExpression(
          Seq(
            (
              LynxPropertyKey("name"),
              LynxStringLiteral("G")
            )
          )
        )
      )
    )

    val relPattern1 = LynxRelationshipPattern(
      LynxVariable("r", 1),
      Seq(LynxRelationshipType("KNOW")),
      0,
      10,
      None,
      SemanticDirection.OUTGOING
    )

    val relationshipScanOperator1 = PathScanOperator(
      relPattern1,
      leftPattern,
      rightPattern,
      model,
      expressionEvaluator,
      ctx.expressionContext
    )
    val resultData =
      getOperatorAllOutputs(relationshipScanOperator1).flatMap(inputBatch => inputBatch.batchData)

    Assert.assertEquals(1, resultData.length)

    val relPattern2 = LynxRelationshipPattern(
      LynxVariable("r", 1),
      Seq(LynxRelationshipType("KNOW")),
      0,
      10,
      None,
      SemanticDirection.INCOMING
    )

    val relationshipScanOperator2 = PathScanOperator(
      relPattern2,
      rightPattern,
      leftPattern,
      model,
      expressionEvaluator,
      ctx.expressionContext
    )
    val resultData2 =
      getOperatorAllOutputs(relationshipScanOperator2).flatMap(inputBatch => inputBatch.batchData)

    Assert.assertEquals(1, resultData2.length)
  }

  @Test
  def testBothHopSearch(): Unit = {
    prepareBothHopsData()
    val leftPattern = LynxNodePattern(LynxVariable("n", 0), Seq.empty, None)
    val rightPattern = LynxNodePattern(LynxVariable("m", 2), Seq.empty, None)
    val relPattern = LynxRelationshipPattern(
      LynxVariable("r", 1),
      Seq(LynxRelationshipType("KNOW")),
      1,
      100,
      None,
      SemanticDirection.BOTH
    )

    val relationshipScanOperator = PathScanOperator(
      relPattern,
      leftPattern,
      rightPattern,
      model,
      expressionEvaluator,
      ctx.expressionContext
    )
    val resultData =
      getOperatorAllOutputs(relationshipScanOperator).flatMap(inputBatch => inputBatch.batchData)

    // 1 hop: 3
    // 2 hop: 1
    Assert.assertEquals(4, resultData.length)

    val relPattern2 = LynxRelationshipPattern(
      LynxVariable("r", 1),
      Seq(LynxRelationshipType("KNOW")),
      0,
      100,
      None,
      SemanticDirection.BOTH
    )

    val relationshipScanOperator2 = PathScanOperator(
      relPattern2,
      leftPattern,
      rightPattern,
      model,
      expressionEvaluator,
      ctx.expressionContext
    )
    val resultData2 =
      getOperatorAllOutputs(relationshipScanOperator2).flatMap(inputBatch => inputBatch.batchData)

    // 0 hop: 4
    // 1 hop: 3
    // 2 hop: 1
    Assert.assertEquals(8, resultData2.length)
  }
}
