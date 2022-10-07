package org.grapheco.lynx.execution

import org.grapheco.lynx.expression.pattern.{LynxNodePattern, LynxRelationshipPattern}
import org.grapheco.lynx.graph.GraphModel
import org.grapheco.lynx.physical.filters.{NodeFilter, RelationshipFilter}
import org.grapheco.lynx.types.composite.LynxMap
import org.grapheco.lynx.types.property.LynxPath
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey, LynxRelationshipType}
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, LynxType, RowBatch}
import org.opencypher.v9_0.expressions.{Expression, LabelName, LogicalVariable, NodePattern, Range, RelTypeName, RelationshipPattern, SemanticDirection}
import org.opencypher.v9_0.util.symbols.{CTList, CTNode, CTRelationship, ListType}

/**
  *@description: PathScanOperator is used to query path in DB,
  *               path has two kind of triple:
  *                  1. single relationship: [leftNode, relationship, rightNode]:
  *                        It means that there is a relationship from leftNode to reach rightNode.
  *                  2. multiple relationships in the middle: [leftNode, relationship*, rightNode]
  *                        It means that rightNode can be reached via a multi-hop path from leftNode.
  */
case class PathScanOperator(
    relPattern: LynxRelationshipPattern,
    leftNodePattern: LynxNodePattern,
    rightNodePattern: LynxNodePattern,
    graphModel: GraphModel,
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  var schema: Seq[(String, LynxType)] = Seq.empty
  var dataSource: Iterator[RowBatch] = Iterator.empty

  override def openImpl(): Unit = {
    schema = {
      if (relPattern.lowerHop == 1 && relPattern.upperHop == 1) {
        Seq(
          leftNodePattern.variable.name -> CTNode,
          relPattern.variable.name -> CTRelationship,
          rightNodePattern.variable.name -> CTNode
        )
      } else {
        Seq(
          leftNodePattern.variable.name -> CTNode,
          relPattern.variable.name -> CTList(
            CTRelationship
          ),
          rightNodePattern.variable.name -> CTNode
        )
      }
    }

    val data = graphModel
      .paths(
        NodeFilter(
          leftNodePattern.labels,
          leftNodePattern.properties
            .map(expr =>
              expressionEvaluator
                .eval(expr)(expressionContext)
                .asInstanceOf[LynxMap]
                .value
                .map(kv => (LynxPropertyKey(kv._1), kv._2))
            )
            .getOrElse(Map.empty)
        ),
        RelationshipFilter(
          relPattern.types,
          relPattern.properties
            .map(expr =>
              expressionEvaluator
                .eval(expr)(expressionContext)
                .asInstanceOf[LynxMap]
                .value
                .map(kv => (LynxPropertyKey(kv._1), kv._2))
            )
            .getOrElse(Map.empty)
        ),
        NodeFilter(
          rightNodePattern.labels,
          rightNodePattern.properties
            .map(expr =>
              expressionEvaluator
                .eval(expr)(expressionContext)
                .asInstanceOf[LynxMap]
                .value
                .map(kv => (LynxPropertyKey(kv._1), kv._2))
            )
            .getOrElse(Map.empty)
        ),
        relPattern.direction,
        Option(relPattern.lowerHop),
        Option(relPattern.upperHop)
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
              Seq(pathTriple.head.startNode, LynxPath(pathTriple), pathTriple.last.endNode)
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
