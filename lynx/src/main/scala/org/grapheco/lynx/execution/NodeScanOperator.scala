package org.grapheco.lynx.execution

import org.grapheco.lynx.expression.pattern.LynxNodePattern
import org.grapheco.lynx.graph.GraphModel
import org.grapheco.lynx.physical.filters.NodeFilter
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, LynxType, RowBatch}
import org.opencypher.v9_0.util.symbols.CTNode

/**
  *@author:John117
  *@createDate:2022/7/29
  *@description:
  */
case class NodeScanOperator(
    pattern: LynxNodePattern,
    graphModel: GraphModel,
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val children: Seq[ExecutionOperator] = Seq.empty

  var schema: Seq[(String, LynxType)] = Seq.empty
  var dataSource: Iterator[RowBatch] = Iterator.empty

  // prepare data
  override def openImpl(): Unit = {
    val LynxNodePattern(variable, nodeLabels, properties) = pattern

    schema = Seq(variable.name -> CTNode)
    dataSource = graphModel
      .nodes(NodeFilter(nodeLabels, properties))
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
