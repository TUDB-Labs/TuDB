package org.grapheco.lynx.operator

import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, GraphModel, LynxType, RowBatch}
import org.opencypher.v9_0.util.symbols.CypherType

/**
  *@author:John117
  *@createDate:2022/8/3
  *@description:
  */
case class SelectOperator(
    columnsToSelect: Seq[(String, Option[String])],
    in: ExecutionOperator,
    graphModel: GraphModel,
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {

  override val exprEvaluator: ExpressionEvaluator = expressionEvaluator
  override val exprContext: ExpressionContext = expressionContext

  var inSchemaWithIndex: Map[String, (CypherType, Int)] = Map.empty
  var outPutSchema: Seq[(String, CypherType)] = Seq.empty

  override def openImpl(): Unit = {
    in.open()
    // columnName -> (CT-Type, index)
    inSchemaWithIndex = in.outputSchema().zipWithIndex.map(x => x._1._1 -> (x._1._2, x._2)).toMap
    outPutSchema = columnsToSelect.map(column =>
      column._2.getOrElse(column._1) -> inSchemaWithIndex(column._1)._1
    )
  }

  override def getNextImpl(): RowBatch = {
    val data = in.getNext()
    val selectedBatchResult = data.batchData.map(rowData => {
      columnsToSelect.map(column => rowData.apply(inSchemaWithIndex(column._1)._2))
    })
    RowBatch(selectedBatchResult)
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = outPutSchema
}
