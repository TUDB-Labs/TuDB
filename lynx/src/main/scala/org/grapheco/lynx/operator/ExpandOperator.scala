package org.grapheco.lynx.operator

import org.grapheco.lynx.types.composite.LynxMap
import org.grapheco.lynx.types.structural.{LynxNode, LynxNodeLabel, LynxPropertyKey, LynxRelationshipType}
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, GraphModel, LynxType, NodeFilter, RelationshipFilter, RowBatch}
import org.opencypher.v9_0.expressions.{Expression, LabelName, LogicalVariable, NodePattern, Range, RelTypeName, RelationshipPattern, SemanticDirection}
import org.opencypher.v9_0.util.symbols.{CTList, CTNode, CTRelationship}

/**
  *@description: This operator is used to expand path.
  *             When we have a path like (a)-[r:know]->(b), If we want to get more relationship from b,
  *             then we can expand from b: (a)-[r:know]->(b)-[x:friend]->(c)
  */
case class ExpandOperator(
    in: ExecutionOperator,
    rel: RelationshipPattern,
    rightNode: NodePattern,
    graphModel: GraphModel,
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val exprEvaluator: ExpressionEvaluator = expressionEvaluator
  override val exprContext: ExpressionContext = expressionContext
  override val children: Seq[ExecutionOperator] = Seq(in)

  var schema: Seq[(String, LynxType)] = Seq.empty

  var relationshipFilter: RelationshipFilter = _
  var rightNodeFilter: NodeFilter = _
  var minLength: Int = _
  var maxLength: Int = _
  var direction: SemanticDirection = _

  override def openImpl(): Unit = {
    in.open()

    val RelationshipPattern(
      relVariable: Option[LogicalVariable],
      relTypes: Seq[RelTypeName],
      length: Option[Option[Range]],
      relProps: Option[Expression],
      relDirection: SemanticDirection,
      legacyTypeSeparator: Boolean,
      baseRel: Option[LogicalVariable]
    ) = rel

    val NodePattern(
      rightNodeVariable,
      rightNodeLabels: Seq[LabelName],
      rightNodeProps: Option[Expression],
      rightBaseNode: Option[LogicalVariable]
    ) = rightNode

    schema = {
      val expandSchema = if (length.isEmpty) {
        Seq(
          relVariable.map(_.name).getOrElse(s"__RELATIONSHIP_${rel.hashCode}") -> CTRelationship,
          rightNodeVariable.map(_.name).getOrElse(s"__NODE_${rightNode.hashCode}") -> CTNode
        )
      } else {
        Seq(
          relVariable.map(_.name).getOrElse(s"__RELATIONSHIP_LIST_${rel.hashCode}") -> CTList(
            CTRelationship
          ),
          rightNodeVariable.map(_.name).getOrElse(s"__NODE_${rightNode.hashCode}") -> CTNode
        )
      }
      in.outputSchema() ++ expandSchema
    }

    val (min, max) = length match {
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
    relationshipFilter = RelationshipFilter(
      relTypes.map(relTypeName => LynxRelationshipType(relTypeName.name)),
      relProps
        .map(expr =>
          evalExpr(expr)(exprContext)
            .asInstanceOf[LynxMap]
            .value
            .map(kv => (LynxPropertyKey(kv._1), kv._2))
        )
        .getOrElse(Map.empty)
    )
    rightNodeFilter = NodeFilter(
      rightNodeLabels.map(labelName => LynxNodeLabel(labelName.name)),
      rightNodeProps
        .map(expr =>
          evalExpr(expr)(exprContext)
            .asInstanceOf[LynxMap]
            .value
            .map(kv => (LynxPropertyKey(kv._1), kv._2))
        )
        .getOrElse(Map.empty)
    )
    direction = relDirection
    minLength = min
    maxLength = max
  }

  override def getNextImpl(): RowBatch = {
    val inBatchData = in.getNext()
    if (inBatchData.batchData.nonEmpty) {
      val expandBatch = inBatchData.batchData.flatMap(path => {
        val lastNode = path.last.asInstanceOf[LynxNode]
        graphModel
          .expand(lastNode, relationshipFilter, rightNodeFilter, direction, minLength, maxLength)
          .map(expandPath =>
            expandPath
              .flatMap(pathTriple => path ++ Seq(pathTriple.storedRelation, pathTriple.endNode))
          )
          .toSeq
      })
      if (expandBatch.nonEmpty) RowBatch(expandBatch)
      else getNextImpl()
    } else RowBatch(Seq.empty)
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = schema
}
