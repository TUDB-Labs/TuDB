package org.grapheco.lynx.operator

import org.grapheco.lynx.types.composite.LynxMap
import org.grapheco.lynx.types.property.LynxPath
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey, LynxRelationshipType}
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, GraphModel, LynxType, NodeFilter, RelationshipFilter, RowBatch}
import org.opencypher.v9_0.expressions.{Expression, LabelName, LogicalVariable, NodePattern, Range, RelTypeName, RelationshipPattern, SemanticDirection}
import org.opencypher.v9_0.util.symbols.{CTList, CTNode, CTRelationship, ListType}

/**
  *@author:John117
  *@createDate:2022/8/1
  *@description:
  */
case class RelationshipScanOperator(
    rel: RelationshipPattern,
    leftNode: NodePattern,
    rightNode: NodePattern,
    graphModel: GraphModel,
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val children: Seq[ExecutionOperator] = Seq.empty
  override val exprEvaluator: ExpressionEvaluator = expressionEvaluator
  override val exprContext: ExpressionContext = expressionContext

  var schema: Seq[(String, LynxType)] = Seq.empty
  var dataSource: Iterator[RowBatch] = Iterator.empty

  override def openImpl(): Unit = {
    val RelationshipPattern(
      relVariable: Option[LogicalVariable],
      types: Seq[RelTypeName],
      length: Option[Option[Range]],
      props2: Option[Expression],
      direction: SemanticDirection,
      legacyTypeSeparator: Boolean,
      baseRel: Option[LogicalVariable]
    ) = rel
    val NodePattern(
      leftNodeVariable,
      labels1: Seq[LabelName],
      props1: Option[Expression],
      baseNode1: Option[LogicalVariable]
    ) = leftNode
    val NodePattern(
      rightNodeVariable,
      labels3: Seq[LabelName],
      props3: Option[Expression],
      baseNode3: Option[LogicalVariable]
    ) = rightNode

    schema = {
      if (length.isEmpty) {
        Seq(
          leftNodeVariable.map(_.name).getOrElse(s"__NODE_${leftNode.hashCode}") -> CTNode,
          relVariable.map(_.name).getOrElse(s"__RELATIONSHIP_${rel.hashCode}") -> CTRelationship,
          rightNodeVariable.map(_.name).getOrElse(s"__NODE_${rightNode.hashCode}") -> CTNode
        )
      } else {
        Seq(
          leftNodeVariable.map(_.name).getOrElse(s"__NODE_${leftNode.hashCode}") -> CTNode,
          relVariable.map(_.name).getOrElse(s"__RELATIONSHIP_LIST_${rel.hashCode}") -> CTList(
            CTRelationship
          ),
          rightNodeVariable.map(_.name).getOrElse(s"__NODE_${rightNode.hashCode}") -> CTNode
        )
      }
    }

    val (lowerLimit, upperLimit) = length match {
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

    val data = graphModel
      .paths(
        NodeFilter(
          labels1.map(labelName => labelName.name).map(LynxNodeLabel),
          props1
            .map(expr =>
              evalExpr(expr)(exprContext)
                .asInstanceOf[LynxMap]
                .value
                .map(kv => (LynxPropertyKey(kv._1), kv._2))
            )
            .getOrElse(Map.empty)
        ),
        RelationshipFilter(
          types.map(relTypeName => relTypeName.name).map(LynxRelationshipType),
          props2
            .map(expr =>
              evalExpr(expr)(exprContext)
                .asInstanceOf[LynxMap]
                .value
                .map(kv => (LynxPropertyKey(kv._1), kv._2))
            )
            .getOrElse(Map.empty)
        ),
        NodeFilter(
          labels3.map(labelName => labelName.name).map(LynxNodeLabel),
          props3
            .map(expr =>
              evalExpr(expr)(exprContext)
                .asInstanceOf[LynxMap]
                .value
                .map(kv => (LynxPropertyKey(kv._1), kv._2))
            )
            .getOrElse(Map.empty)
        ),
        direction,
        Option(upperLimit),
        Option(lowerLimit)
      )

    val relCypherType = schema(1)._2 // get relationship's CT-Type: is CTRelationship or ListType(CTRelationship)
    dataSource = relCypherType match {
      case CTRelationship => {
        data
          .grouped(numRowsPerBatch)
          .map(batch =>
            batch.map(triple =>
              Seq(triple.head.startNode, triple.head.storedRelation, triple.head.endNode)
            )
          )
          .map(f => RowBatch(f))
      }
      // process relationship Path to support like (a)-[r:TYPE*1..3]->(b)
      case ListType(CTRelationship) => {
        data
          .grouped(numRowsPerBatch)
          .map(batch =>
            batch.map(pathTriple =>
              Seq(pathTriple.head.startNode, LynxPath(pathTriple), pathTriple.head.endNode)
            )
          )
          .map(f => RowBatch(f))
      }
    }
  }

  override def getNextImpl(): RowBatch = {
    if (dataSource.nonEmpty) dataSource.next()
    else RowBatch(Seq.empty)
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = schema
}
