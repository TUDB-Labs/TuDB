package org.grapheco.lynx.operator

import org.grapheco.lynx.procedure.ProcedureExpression
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.property.{LynxBoolean, LynxInteger, LynxNull, LynxString}
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey, LynxRelationshipType}
import org.junit.{Assert, Test}
import org.opencypher.v9_0.ast.{SetExactPropertiesFromMapItem, SetIncludingPropertiesFromMapItem, SetLabelItem, SetPropertyItem}
import org.opencypher.v9_0.expressions.{CaseExpression, Equals, FunctionInvocation, FunctionName, LabelName, MapExpression, Namespace, Null, Property, PropertyKeyName, SignedDecimalIntegerLiteral, StringLiteral, True, Variable}

class SetOperatorTest extends BaseOperatorTest {
  val node1 = TestNode(
    TestId(1L),
    Seq(LynxNodeLabel("Person")),
    Map(LynxPropertyKey("name") -> LynxValue("Stefan"))
  )
  val node2 = TestNode(
    TestId(2L),
    Seq(LynxNodeLabel("Person")),
    Map(LynxPropertyKey("name") -> LynxValue("George"))
  )
  val node3 = TestNode(
    TestId(3L),
    Seq(LynxNodeLabel("Swedish")),
    Map(
      LynxPropertyKey("name") -> LynxValue("Andy"),
      LynxPropertyKey("age") -> LynxInteger(36),
      LynxPropertyKey("hungry") -> LynxBoolean(true)
    )
  )
  val node4 = TestNode(
    TestId(4L),
    Seq(LynxNodeLabel("Person")),
    Map(LynxPropertyKey("name") -> LynxValue("Peter"), LynxPropertyKey("age") -> LynxInteger(34))
  )
  val rel1 = TestRelationship(
    TestId(1L),
    TestId(1L),
    TestId(3L),
    Option(LynxRelationshipType("KNOWS")),
    Map.empty
  )
  val rel2 = TestRelationship(
    TestId(2L),
    TestId(2L),
    TestId(4L),
    Option(LynxRelationshipType("KNOWS")),
    Map.empty
  )
  val rel3 = TestRelationship(
    TestId(3L),
    TestId(3L),
    TestId(4L),
    Option(LynxRelationshipType("KNOWS")),
    Map.empty
  )

  all_nodes.append(node1, node2, node3, node4)
  all_rels.append(rel1, rel2, rel3)

  @Test
  def testSetAProperty(): Unit = {
    /*
      MATCH (n {name: 'Andy'})
      SET n.surname = 'Taylor'
      RETURN n.name, n.surname
     */
    val projectColumn = Seq(
      (
        "n.name",
        Property(
          Variable("n")(defaultPosition),
          PropertyKeyName("name")(defaultPosition)
        )(defaultPosition)
      ),
      (
        "n.surname",
        Property(
          Variable("n")(defaultPosition),
          PropertyKeyName("surname")(defaultPosition)
        )(defaultPosition)
      )
    )
    val setItems = Seq(
      SetPropertyItem(
        Property(Variable("n")(defaultPosition), PropertyKeyName("surname")(defaultPosition))(
          defaultPosition
        ),
        StringLiteral("Taylor")(defaultPosition)
      )(defaultPosition)
    )
    val nodeScanOperator = prepareNodeScanOperator(
      "n",
      Seq.empty,
      Seq((PropertyKeyName("name")(defaultPosition), StringLiteral("Andy")(defaultPosition)))
    )
    val setOperator =
      SetOperator(nodeScanOperator, setItems, model, expressionEvaluator, ctx.expressionContext)
    val projectOperator =
      ProjectOperator(setOperator, projectColumn, expressionEvaluator, ctx.expressionContext)

    val res = getOperatorAllOutputs(projectOperator).head.batchData.flatten
    Assert.assertEquals(Seq(LynxString("Andy"), LynxString("Taylor")), res)
  }

  @Test
  def testSetCaseTrueProperty(): Unit = {
    /*
        MATCH (n {name:'Andy'})
        SET (CASE WHEN n.age = 36 THEN n END).worksIn = 'Malmo'
        RETURN n.name, n.worksIn
     */
    val projectColumn = Seq(
      (
        "n.name",
        Property(
          Variable("n")(defaultPosition),
          PropertyKeyName("name")(defaultPosition)
        )(defaultPosition)
      ),
      (
        "n.worksIn",
        Property(
          Variable("n")(defaultPosition),
          PropertyKeyName("worksIn")(defaultPosition)
        )(defaultPosition)
      )
    )
    val setItems = Seq(
      SetPropertyItem(
        Property(
          CaseExpression(
            None,
            List(
              (
                Equals(
                  Property(
                    Variable("n")(defaultPosition),
                    PropertyKeyName("age")(defaultPosition)
                  )(defaultPosition),
                  SignedDecimalIntegerLiteral("36")(defaultPosition)
                )(defaultPosition),
                Variable("n")(defaultPosition)
              )
            ),
            None
          )(defaultPosition),
          PropertyKeyName("worksIn")(defaultPosition)
        )(
          defaultPosition
        ),
        StringLiteral("Malmo")(defaultPosition)
      )(defaultPosition)
    )
    val nodeScanOperator = prepareNodeScanOperator(
      "n",
      Seq.empty,
      Seq((PropertyKeyName("name")(defaultPosition), StringLiteral("Andy")(defaultPosition)))
    )
    val setOperator =
      SetOperator(nodeScanOperator, setItems, model, expressionEvaluator, ctx.expressionContext)
    val projectOperator =
      ProjectOperator(setOperator, projectColumn, expressionEvaluator, ctx.expressionContext)

    val res = getOperatorAllOutputs(projectOperator).head.batchData.flatten
    Assert.assertEquals(Seq(LynxString("Andy"), LynxString("Malmo")), res)
  }

  @Test
  def testSetCaseFalseProperty(): Unit = {
    /*
        MATCH (n {name:'Andy'})
        SET (CASE WHEN n.age = 55 THEN n END).worksIn = 'Malmo'
        RETURN n.name, n.worksIn
     */
    val projectColumn = Seq(
      (
        "n.name",
        Property(
          Variable("n")(defaultPosition),
          PropertyKeyName("name")(defaultPosition)
        )(defaultPosition)
      ),
      (
        "n.worksIn",
        Property(
          Variable("n")(defaultPosition),
          PropertyKeyName("worksIn")(defaultPosition)
        )(defaultPosition)
      )
    )
    val setItems = Seq(
      SetPropertyItem(
        Property(
          CaseExpression(
            None,
            List(
              (
                Equals(
                  Property(
                    Variable("n")(defaultPosition),
                    PropertyKeyName("age")(defaultPosition)
                  )(defaultPosition),
                  SignedDecimalIntegerLiteral("55")(defaultPosition)
                )(defaultPosition),
                Variable("n")(defaultPosition)
              )
            ),
            None
          )(defaultPosition),
          PropertyKeyName("worksIn")(defaultPosition)
        )(
          defaultPosition
        ),
        StringLiteral("Malmo")(defaultPosition)
      )(defaultPosition)
    )
    val nodeScanOperator = prepareNodeScanOperator(
      "n",
      Seq.empty,
      Seq((PropertyKeyName("name")(defaultPosition), StringLiteral("Andy")(defaultPosition)))
    )
    val setOperator =
      SetOperator(nodeScanOperator, setItems, model, expressionEvaluator, ctx.expressionContext)
    val projectOperator =
      ProjectOperator(setOperator, projectColumn, expressionEvaluator, ctx.expressionContext)

    val res = getOperatorAllOutputs(projectOperator).head.batchData.flatten
    Assert.assertEquals(Seq(LynxString("Andy"), LynxNull), res)
  }

  @Test
  def testUpdateAProperty(): Unit = {
    /*
      MATCH (n {name: 'Andy'})
      SET n.age = toString(n.age)
      RETURN n.name, n.age
     */
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
    val setItems = Seq(
      SetPropertyItem(
        Property(Variable("n")(defaultPosition), PropertyKeyName("age")(defaultPosition))(
          defaultPosition
        ),
        ProcedureExpression(
          FunctionInvocation(
            Namespace(List())(defaultPosition),
            FunctionName("toString")(defaultPosition),
            false,
            IndexedSeq(
              Property(Variable("n")(defaultPosition), PropertyKeyName("age")(defaultPosition))(
                defaultPosition
              )
            )
          )(defaultPosition)
        )(runnerContext)
      )(defaultPosition)
    )
    val nodeScanOperator = prepareNodeScanOperator(
      "n",
      Seq.empty,
      Seq((PropertyKeyName("name")(defaultPosition), StringLiteral("Andy")(defaultPosition)))
    )
    val setOperator =
      SetOperator(nodeScanOperator, setItems, model, expressionEvaluator, ctx.expressionContext)
    val projectOperator =
      ProjectOperator(setOperator, projectColumn, expressionEvaluator, ctx.expressionContext)

    val res = getOperatorAllOutputs(projectOperator).head.batchData.flatten
    Assert.assertEquals(Seq(LynxString("Andy"), LynxString("36")), res)
  }

  @Test
  def testRemoveAProperty(): Unit = {
    /*
      MATCH (n {name: 'Andy'})
      SET n.age = null
      RETURN n.name, n.age
     */
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
    val setItems = Seq(
      SetPropertyItem(
        Property(Variable("n")(defaultPosition), PropertyKeyName("name")(defaultPosition))(
          defaultPosition
        ),
        Null()(defaultPosition)
      )(defaultPosition)
    )
    val nodeScanOperator = prepareNodeScanOperator(
      "n",
      Seq.empty,
      Seq((PropertyKeyName("name")(defaultPosition), StringLiteral("Andy")(defaultPosition)))
    )
    val setOperator =
      SetOperator(nodeScanOperator, setItems, model, expressionEvaluator, ctx.expressionContext)
    val projectOperator =
      ProjectOperator(setOperator, projectColumn, expressionEvaluator, ctx.expressionContext)

    val res = getOperatorAllOutputs(projectOperator).head.batchData.flatten
    Assert.assertEquals(Seq(LynxNull, LynxInteger(36)), res)
  }

  @Test
  def testCopyPropertiesBetweenNodesAndRelationships(): Unit = {
    /*
        MATCH (at {name: 'Andy'}),(pn {name: 'Peter'})
        SET at = pn
        RETURN at.name, at.age, at.hungry, pn.name, pn.age
     */
    // TODO: wait joinOperator
  }

  @Test
  def testReplaceAllPropertiesUsingAMapAndEqualSign(): Unit = {
    /*
      MATCH (p {name: 'Peter'})
      SET p = {name: 'Peter Smith', position: 'Entrepreneur'}
      RETURN p.name, p.age, p.position
     */
    val projectColumn = Seq(
      (
        "p.name",
        Property(
          Variable("p")(defaultPosition),
          PropertyKeyName("name")(defaultPosition)
        )(defaultPosition)
      ),
      (
        "p.age",
        Property(
          Variable("p")(defaultPosition),
          PropertyKeyName("age")(defaultPosition)
        )(defaultPosition)
      ),
      (
        "p.position",
        Property(
          Variable("p")(defaultPosition),
          PropertyKeyName("position")(defaultPosition)
        )(defaultPosition)
      )
    )
    val setItems = Seq(
      SetExactPropertiesFromMapItem(
        Variable("p")(defaultPosition),
        MapExpression(
          Seq(
            (
              PropertyKeyName("name")(defaultPosition),
              StringLiteral("Peter Smith")(defaultPosition)
            ),
            (
              PropertyKeyName("position")(defaultPosition),
              StringLiteral("Entrepreneur")(defaultPosition)
            )
          )
        )(defaultPosition)
      )(defaultPosition)
    )

    val nodeScanOperator = prepareNodeScanOperator(
      "p",
      Seq.empty,
      Seq((PropertyKeyName("name")(defaultPosition), StringLiteral("Peter")(defaultPosition)))
    )
    val setOperator =
      SetOperator(nodeScanOperator, setItems, model, expressionEvaluator, ctx.expressionContext)
    val projectOperator =
      ProjectOperator(setOperator, projectColumn, expressionEvaluator, ctx.expressionContext)

    val res = getOperatorAllOutputs(projectOperator).head.batchData.flatten
    Assert.assertEquals(Seq(LynxString("Peter Smith"), LynxNull, LynxString("Entrepreneur")), res)
  }

  @Test
  def testRemoveAllPropertiesUsingAnEmptyMapAndEqualSign(): Unit = {
    /*
        MATCH (p {name: 'Peter'})
        SET p = {}
        RETURN p.name, p.age
     */
    val projectColumn = Seq(
      (
        "p.name",
        Property(
          Variable("p")(defaultPosition),
          PropertyKeyName("name")(defaultPosition)
        )(defaultPosition)
      ),
      (
        "p.age",
        Property(
          Variable("p")(defaultPosition),
          PropertyKeyName("age")(defaultPosition)
        )(defaultPosition)
      )
    )
    val setItems = Seq(
      SetExactPropertiesFromMapItem(
        Variable("p")(defaultPosition),
        MapExpression(Seq())(defaultPosition)
      )(defaultPosition)
    )

    val nodeScanOperator = prepareNodeScanOperator(
      "p",
      Seq.empty,
      Seq((PropertyKeyName("name")(defaultPosition), StringLiteral("Peter")(defaultPosition)))
    )
    val setOperator =
      SetOperator(nodeScanOperator, setItems, model, expressionEvaluator, ctx.expressionContext)
    val projectOperator =
      ProjectOperator(setOperator, projectColumn, expressionEvaluator, ctx.expressionContext)

    val res = getOperatorAllOutputs(projectOperator).head.batchData.flatten
    Assert.assertEquals(Seq(LynxNull, LynxNull), res)
  }

  @Test
  def testMutateSpecificPropertiesUsingAMapAndEqualSign(): Unit = {
    /*
      MATCH (p {name: 'Peter'})
      SET p += {age: 38, hungry: true, position: 'Entrepreneur'}
      RETURN p.name, p.age, p.hungry, p.position
     */
    val projectColumn = Seq(
      (
        "p.name",
        Property(
          Variable("p")(defaultPosition),
          PropertyKeyName("name")(defaultPosition)
        )(defaultPosition)
      ),
      (
        "p.age",
        Property(
          Variable("p")(defaultPosition),
          PropertyKeyName("age")(defaultPosition)
        )(defaultPosition)
      ),
      (
        "p.hungry",
        Property(
          Variable("p")(defaultPosition),
          PropertyKeyName("hungry")(defaultPosition)
        )(defaultPosition)
      ),
      (
        "p.position",
        Property(
          Variable("p")(defaultPosition),
          PropertyKeyName("position")(defaultPosition)
        )(defaultPosition)
      )
    )
    val setItems = Seq(
      SetIncludingPropertiesFromMapItem(
        Variable("p")(defaultPosition),
        MapExpression(
          Seq(
            (
              PropertyKeyName("age")(defaultPosition),
              SignedDecimalIntegerLiteral("38")(defaultPosition)
            ),
            (PropertyKeyName("hungry")(defaultPosition), True()(defaultPosition)),
            (
              PropertyKeyName("position")(defaultPosition),
              StringLiteral("Entrepreneur")(defaultPosition)
            )
          )
        )(defaultPosition)
      )(defaultPosition)
    )

    val nodeScanOperator = prepareNodeScanOperator(
      "p",
      Seq.empty,
      Seq((PropertyKeyName("name")(defaultPosition), StringLiteral("Peter")(defaultPosition)))
    )
    val setOperator =
      SetOperator(nodeScanOperator, setItems, model, expressionEvaluator, ctx.expressionContext)
    val projectOperator =
      ProjectOperator(setOperator, projectColumn, expressionEvaluator, ctx.expressionContext)

    val res = getOperatorAllOutputs(projectOperator).head.batchData.flatten
    Assert.assertEquals(
      Seq(LynxString("Peter"), LynxInteger(38), LynxBoolean(true), LynxString("Entrepreneur")),
      res
    )
  }
  @Test
  def testMutateSpecificPropertiesUsingAMapAndEqualSign2(): Unit = {
    /*
      MATCH (p {name: 'Peter'})
      SET p += {}
      RETURN p.name, p.age
     */
    val projectColumn = Seq(
      (
        "p.name",
        Property(
          Variable("p")(defaultPosition),
          PropertyKeyName("name")(defaultPosition)
        )(defaultPosition)
      ),
      (
        "p.age",
        Property(
          Variable("p")(defaultPosition),
          PropertyKeyName("age")(defaultPosition)
        )(defaultPosition)
      )
    )
    val setItems = Seq(
      SetIncludingPropertiesFromMapItem(
        Variable("p")(defaultPosition),
        MapExpression(Seq())(defaultPosition)
      )(defaultPosition)
    )

    val nodeScanOperator = prepareNodeScanOperator(
      "p",
      Seq.empty,
      Seq((PropertyKeyName("name")(defaultPosition), StringLiteral("Peter")(defaultPosition)))
    )
    val setOperator =
      SetOperator(nodeScanOperator, setItems, model, expressionEvaluator, ctx.expressionContext)
    val projectOperator =
      ProjectOperator(setOperator, projectColumn, expressionEvaluator, ctx.expressionContext)

    val res = getOperatorAllOutputs(projectOperator).head.batchData.flatten
    Assert.assertEquals(
      Seq(LynxString("Peter"), LynxInteger(34)),
      res
    )
  }
  @Test
  def testSetMultiplePropertiesUsingOneSetClause(): Unit = {
    /*
      MATCH (n {name: 'Andy'})
      SET n.position = 'Developer', n.surname = 'Taylor'
     */
    val projectColumn = Seq(
      (
        "n.position",
        Property(
          Variable("n")(defaultPosition),
          PropertyKeyName("position")(defaultPosition)
        )(defaultPosition)
      ),
      (
        "n.surname",
        Property(
          Variable("n")(defaultPosition),
          PropertyKeyName("surname")(defaultPosition)
        )(defaultPosition)
      )
    )
    val setItems = Seq(
      SetPropertyItem(
        Property(Variable("n")(defaultPosition), PropertyKeyName("position")(defaultPosition))(
          defaultPosition
        ),
        StringLiteral("Developer")(defaultPosition)
      )(defaultPosition),
      SetPropertyItem(
        Property(Variable("n")(defaultPosition), PropertyKeyName("surname")(defaultPosition))(
          defaultPosition
        ),
        StringLiteral("Taylor")(defaultPosition)
      )(defaultPosition)
    )
    val nodeScanOperator = prepareNodeScanOperator(
      "n",
      Seq.empty,
      Seq((PropertyKeyName("name")(defaultPosition), StringLiteral("Andy")(defaultPosition)))
    )
    val setOperator =
      SetOperator(nodeScanOperator, setItems, model, expressionEvaluator, ctx.expressionContext)
    val projectOperator =
      ProjectOperator(setOperator, projectColumn, expressionEvaluator, ctx.expressionContext)

    val res = getOperatorAllOutputs(projectOperator).head.batchData.flatten
    Assert.assertEquals(Seq(LynxString("Developer"), LynxString("Taylor")), res)
  }

  @Test
  def testSetALabelOnNode(): Unit = {
    /*
        MATCH (n {name: 'Stefan'})
        SET n:German
        RETURN n.name, labels(n) AS labels
     */
    val projectColumn = Seq(
      (
        "n.name",
        Property(
          Variable("n")(defaultPosition),
          PropertyKeyName("name")(defaultPosition)
        )(defaultPosition)
      ),
      (
        "labels",
        ProcedureExpression(
          FunctionInvocation(
            Namespace()(defaultPosition),
            FunctionName("labels")(defaultPosition),
            false,
            IndexedSeq(Variable("n")(defaultPosition))
          )(defaultPosition)
        )(runnerContext)
      )
    )
    val setItems = Seq(
      SetLabelItem(Variable("n")(defaultPosition), Seq(LabelName("German")(defaultPosition)))(
        defaultPosition
      )
    )
    val nodeScanOperator = prepareNodeScanOperator(
      "n",
      Seq.empty,
      Seq((PropertyKeyName("name")(defaultPosition), StringLiteral("Stefan")(defaultPosition)))
    )
    val setOperator =
      SetOperator(nodeScanOperator, setItems, model, expressionEvaluator, ctx.expressionContext)
    val projectOperator =
      ProjectOperator(setOperator, projectColumn, expressionEvaluator, ctx.expressionContext)

    val res = getOperatorAllOutputs(projectOperator).head.batchData.flatten
    Assert.assertEquals(
      Seq(LynxString("Stefan"), LynxList(List(LynxString("Person"), LynxString("German")))),
      res
    )
  }
  @Test
  def testSetMultipleLabelsOnANode(): Unit = {
    /*
      MATCH (n {name: 'George'})
      SET n:Swedish:Bossman
      RETURN n.name, labels(n) AS labels
     */
    val projectColumn = Seq(
      (
        "n.name",
        Property(
          Variable("n")(defaultPosition),
          PropertyKeyName("name")(defaultPosition)
        )(defaultPosition)
      ),
      (
        "labels",
        ProcedureExpression(
          FunctionInvocation(
            Namespace()(defaultPosition),
            FunctionName("labels")(defaultPosition),
            false,
            IndexedSeq(Variable("n")(defaultPosition))
          )(defaultPosition)
        )(runnerContext)
      )
    )
    val setItems = Seq(
      SetLabelItem(
        Variable("n")(defaultPosition),
        Seq(LabelName("Swedish")(defaultPosition), LabelName("Bossman")(defaultPosition))
      )(
        defaultPosition
      )
    )
    val nodeScanOperator = prepareNodeScanOperator(
      "n",
      Seq.empty,
      Seq((PropertyKeyName("name")(defaultPosition), StringLiteral("George")(defaultPosition)))
    )
    val setOperator =
      SetOperator(nodeScanOperator, setItems, model, expressionEvaluator, ctx.expressionContext)
    val projectOperator =
      ProjectOperator(setOperator, projectColumn, expressionEvaluator, ctx.expressionContext)

    val res = getOperatorAllOutputs(projectOperator).head.batchData.flatten
    Assert.assertEquals(
      Seq(
        LynxString("George"),
        LynxList(List(LynxString("Person"), LynxString("Swedish"), LynxString("Bossman")))
      ),
      res
    )
  }
}
