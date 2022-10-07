package org.grapheco.lynx.execution

import org.grapheco.lynx.expression.pattern.{LynxNodePattern, LynxRelationshipPattern}
import org.grapheco.lynx.graph.GraphModel
import org.grapheco.lynx.physical.filters.{NodeFilter, RelationshipFilter}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxMap
import org.grapheco.lynx.types.structural.{LynxNode, LynxNodeLabel, LynxPropertyKey, LynxRelationshipType}
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, LynxType, RowBatch}
import org.opencypher.v9_0.expressions.{NodePattern, Range, RelationshipPattern}
import org.opencypher.v9_0.util.symbols.{CTList, CTNode, CTRelationship}

/**
  *@description: This operator is used to expand path.
  *             When we have a path like (a)-[r:know]->(b), If we want to get more relationship from b,
  *             then we can expand from b: (a)-[r:know]->(b)-[x:friend]->(c)
  */
case class ExpandOperator(
    in: ExecutionOperator,
    relPattern: LynxRelationshipPattern,
    rightNodePattern: LynxNodePattern,
    graphModel: GraphModel,
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val children: Seq[ExecutionOperator] = Seq(in)

  var schema: Seq[(String, LynxType)] = Seq.empty

  override def openImpl(): Unit = {
    in.open()

    schema = {
      val expandSchema = if (relPattern.lowerHop == 1 && relPattern.upperHop == 1) {
        Seq(
          relPattern.variable.name -> CTRelationship,
          rightNodePattern.variable.name -> CTNode
        )
      } else {
        Seq(
          relPattern.variable.name -> CTList(CTRelationship),
          rightNodePattern.variable.name -> CTNode
        )
      }
      in.outputSchema() ++ expandSchema
    }
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
      relPattern.types,
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
      rightNodePattern.labels,
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
        relPattern.lowerHop,
        relPattern.upperHop
      )
      .map(expandPath =>
        expandPath
          .flatMap(pathTriple => path ++ Seq(pathTriple.storedRelation, pathTriple.endNode))
      )
      .toSeq
  }
}
