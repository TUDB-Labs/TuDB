package org.grapheco.lynx.operator

import org.grapheco.lynx.types.composite.LynxMap
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey}
import org.grapheco.lynx.{CypherRunnerContext, ExecutionContext, ExecutionOperator, ExpressionContext, ExpressionEvaluator, GraphModel, LynxType, NodeFilter, PhysicalPlannerContext, RowBatch, TreeNode}
import org.opencypher.v9_0.expressions.{Expression, LabelName, LogicalVariable, NodePattern}
import org.opencypher.v9_0.util.symbols.CTNode

/**
  *@author:John117
  *@createDate:2022/7/29
  *@description:
  */
case class NodeScanOperator(
    pattern: NodePattern,
    graphModel: GraphModel,
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val children: Seq[ExecutionOperator] = Seq.empty
  override val exprEvaluator: ExpressionEvaluator = expressionEvaluator
  override val exprContext: ExpressionContext = expressionContext

  var schema: Seq[(String, LynxType)] = Seq.empty
  var dataSource: Iterator[RowBatch] = Iterator.empty

  // prepare data
  override def openImpl(): Unit = {
    val NodePattern(
      Some(nodeVariable: LogicalVariable),
      labels: Seq[LabelName],
      properties: Option[Expression],
      baseNode: Option[LogicalVariable]
    ) = pattern

    schema = Seq(nodeVariable.name -> CTNode)

    val nodeLabels = {
      if (labels.nonEmpty) labels.map(_.name).map(LynxNodeLabel)
      else Seq.empty
    }
    dataSource = graphModel
      .nodes(
        NodeFilter(
          nodeLabels,
          properties
            .map(prop =>
              evalExpr(prop)(exprContext)
                .asInstanceOf[LynxMap]
                .value
                .map(kv => (LynxPropertyKey(kv._1), kv._2))
            )
            .getOrElse(Map.empty)
        )
      )
      .grouped(numRowsPerBatch)
      .map(node => Seq(node))
      .map(f => RowBatch(f))
  }

  override def getNextImpl(): RowBatch = {
    if (dataSource.nonEmpty) dataSource.next()
    else RowBatch(Seq.empty)
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = schema

}
