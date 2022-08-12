package org.grapheco.lynx.operator

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxNumber
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, LynxType, RowBatch}
import org.grapheco.tudb.exception.{TuDBError, TuDBException}
import org.opencypher.v9_0.expressions.Expression

import scala.collection.mutable.ArrayBuffer

/**
  *@description: This operator is used to skip data. eg: [1,2,3,4], skip 2, then return [3,4]
  */
case class SkipOperator(
    skipDataSize: Int,
    in: ExecutionOperator,
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val exprEvaluator: ExpressionEvaluator = expressionEvaluator
  override val exprContext: ExpressionContext = expressionContext

  val outputRows: ArrayBuffer[Seq[LynxValue]] = ArrayBuffer.empty
  var isSkipped: Boolean = false
  var skippedDataSize: Int = 0
  override def openImpl(): Unit = {
    in.open()
  }

  override def getNextImpl(): RowBatch = {
    if (!isSkipped) {
      var dataBatch = in.getNext().batchData
      var returnData: Seq[Seq[LynxValue]] = Seq.empty
      while (dataBatch.nonEmpty && skippedDataSize <= skipDataSize) {
        val length = dataBatch.length
        val tmpLength = skippedDataSize + length
        if (tmpLength <= skipDataSize) dataBatch = in.getNext().batchData
        else {
          val offset = skipDataSize - skippedDataSize
          returnData = dataBatch.slice(offset, dataBatch.length)
        }
        skippedDataSize = tmpLength
      }
      isSkipped = true
      RowBatch(returnData)
    } else in.getNext()
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = in.outputSchema()
}
