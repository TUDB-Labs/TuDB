package org.grapheco.lynx.operator

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxMap
import org.grapheco.lynx.types.structural.{LynxNode, LynxNodeLabel, LynxPropertyKey, LynxRelationshipType}
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, GraphModel, LynxType, NodeFilter, RelationshipFilter, RowBatch}
import org.opencypher.v9_0.expressions.{NodePattern, Range, RelationshipPattern}
import org.opencypher.v9_0.util.symbols.{CTList, CTNode, CTRelationship}

/**
  *@description: This operator is used to expand path.
  *             When we have a path like (a)-[r:know]->(b), If we want to get more relationship from b,
  *             then we can expand from b: (a)-[r:know]->(b)-[x:friend]->(c)
  */
case class ExpandOperator(
    in: ExecutionOperator,
    relPattern: RelationshipPattern,
    rightNodePattern: NodePattern,
    graphModel: GraphModel,
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val exprEvaluator: ExpressionEvaluator = expressionEvaluator
  override val exprContext: ExpressionContext = expressionContext
  override val children: Seq[ExecutionOperator] = Seq(in)

  var schema: Seq[(String, LynxType)] = Seq.empty

  var minLength: Int = _
  var maxLength: Int = _

  override def openImpl(): Unit = {
    in.open()

    schema = {
      val expandSchema = if (relPattern.length.isEmpty) {
        Seq(
          relPattern.variable
            .map(_.name)
            .getOrElse(s"__RELATIONSHIP_${relPattern.hashCode}") -> CTRelationship,
          rightNodePattern.variable
            .map(_.name)
            .getOrElse(s"__NODE_${rightNodePattern.hashCode}") -> CTNode
        )
      } else {
        Seq(
          relPattern.variable
            .map(_.name)
            .getOrElse(s"__RELATIONSHIP_LIST_${relPattern.hashCode}") -> CTList(
            CTRelationship
          ),
          rightNodePattern.variable
            .map(_.name)
            .getOrElse(s"__NODE_${rightNodePattern.hashCode}") -> CTNode
        )
      }
      in.outputSchema() ++ expandSchema
    }

    val (min, max) = relPattern.length match {
      case None       => (1, 1)
      case Some(None) => (1, Int.MaxValue)
      case Some(Some(Range(a, b))) => {
        (a, b) match {
          case (_, None) => (a.get.value.toInt, Int.MaxValue)
          case (None, _) => (1, b.get.value.toInt)
          case _         => (a.get.value.toInt, b.get.value.toInt)
        }
      }
    }
    minLength = min
    maxLength = max
  }

  override def getNextImpl(): RowBatch = {
    var inBatchData: Seq[Seq[LynxValue]] = Seq.empty
    var expandedResult: Seq[Seq[LynxValue]] = Seq.empty
    do {
      inBatchData = in.getNext().batchData
      if (inBatchData.isEmpty) return RowBatch(Seq.empty)
      expandedResult = inBatchData.flatMap(path => expandPath(path))
    } while (expandedResult.isEmpty)

    RowBatch(expandedResult)
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = schema

  private def expandPath(path: Seq[LynxValue]): Seq[Seq[LynxValue]] = {
    val ctxMap = schema.map(f => f._1).zip(path).toMap
    val ctx = expressionContext.withVars(ctxMap)

    val relationshipFilter = RelationshipFilter(
      relPattern.types.map(relTypeName => LynxRelationshipType(relTypeName.name)),
      relPattern.properties
        .map(expr =>
          expressionEvaluator
            .eval(expr)(ctx)
            .asInstanceOf[LynxMap]
            .value
            .map(kv => (LynxPropertyKey(kv._1), kv._2))
        )
        .getOrElse(Map.empty)
    )
    val rightNodeFilter = NodeFilter(
      rightNodePattern.labels.map(labelName => LynxNodeLabel(labelName.name)),
      rightNodePattern.properties
        .map(expr =>
          expressionEvaluator
            .eval(expr)(ctx)
            .asInstanceOf[LynxMap]
            .value
            .map(kv => (LynxPropertyKey(kv._1), kv._2))
        )
        .getOrElse(Map.empty)
    )

    val lastNode = path.last.asInstanceOf[LynxNode]
    graphModel
      .expand(
        lastNode,
        relationshipFilter,
        rightNodeFilter,
        relPattern.direction,
        minLength,
        maxLength
      )
      .map(expandPath =>
        expandPath
          .flatMap(pathTriple => path ++ Seq(pathTriple.storedRelation, pathTriple.endNode))
      )
      .toSeq
  }
}
