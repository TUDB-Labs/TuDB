package org.grapheco.lynx.operator

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxNumber
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, LynxType, RowBatch}
import org.grapheco.tudb.exception.{TuDBError, TuDBException}
import org.opencypher.v9_0.expressions.Expression

import scala.collection.mutable.ArrayBuffer

/**
  *@description: This operator is used to limit return data. eg: [1,2,3,4], limit 2, then return [1, 2]
  */
case class LimitOperator(
    limitDataSize: Int,
    in: ExecutionOperator,
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val exprEvaluator: ExpressionEvaluator = expressionEvaluator
  override val exprContext: ExpressionContext = expressionContext
  var limitNum: Int = 0
  var isFinishLimit: Boolean = false
  var allGroupedLimitedDataArray: Iterator[Seq[Seq[LynxValue]]] = Iterator.empty

  override def openImpl(): Unit = {
    in.open()
  }

  override def getNextImpl(): RowBatch = {
    if (!isFinishLimit) {
      val allLimitedDataArray: ArrayBuffer[Seq[LynxValue]] = ArrayBuffer.empty
      var dataBatch = in.getNext().batchData
      while (dataBatch.nonEmpty && allLimitedDataArray.length < limitDataSize) {
        allLimitedDataArray.append(dataBatch: _*)
        dataBatch = in.getNext().batchData
      }
      allGroupedLimitedDataArray =
        allLimitedDataArray.slice(0, limitDataSize).grouped(numRowsPerBatch)
      isFinishLimit = true
    }
    if (allGroupedLimitedDataArray.nonEmpty) RowBatch(allGroupedLimitedDataArray.next())
    else RowBatch(Seq.empty)
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = in.outputSchema()
}
