package org.grapheco.lynx.operator

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.{LynxBoolean, LynxNull}
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, LynxType, RowBatch}
import org.opencypher.v9_0.expressions.Expression

import scala.collection.mutable.ArrayBuffer

/**
  *@author:John117
  *@createDate:2022/8/1
  *@description:
  */
case class FilterOperator(
    filterExpr: Expression,
    in: ExecutionOperator,
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val children: Seq[ExecutionOperator] = Seq(in)
  override val exprEvaluator: ExpressionEvaluator = expressionEvaluator
  override val exprContext: ExpressionContext = expressionContext

  var rowSchemaName: Seq[String] = Seq.empty
  val filteredDataArray: ArrayBuffer[Seq[LynxValue]] = ArrayBuffer()

  override def openImpl(): Unit = {
    in.open()
    rowSchemaName = in.outputSchema().map(nameAndType => nameAndType._1)
  }

  override def getNextImpl(): RowBatch = {
    val data = getFilteredBatchData()
    if (data.nonEmpty) RowBatch(data)
    else RowBatch(Seq.empty)
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = in.outputSchema()

  private def getFilteredBatchData(): Seq[Seq[LynxValue]] = {
    filteredDataArray.clear()
    var nextBatchData = in.getNext()
    while (filteredDataArray.length < numRowsPerBatch && nextBatchData.batchData.nonEmpty) {
      val filteredData = nextBatchData.batchData.filter(rowData => {
        // FIXME: eval function can only process data row by row, cannot batch process.
        evalExpr(filterExpr)(exprContext.withVars(rowSchemaName.zip(rowData).toMap)) match {
          case LynxBoolean(v) => v
          case LynxNull       => false
        }
      })
      filteredDataArray.append(filteredData: _*)
      if (filteredDataArray.length < numRowsPerBatch) nextBatchData = in.getNext()
    }
    filteredDataArray.toArray.toSeq
  }
}
