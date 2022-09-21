package org.grapheco.lynx.operator

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, LynxType, RowBatch}
import scala.collection.mutable.ArrayBuffer

/**
  *@description: This operator is used to skip data. eg: [1,2,3,4], skip 2, then return [3,4]
  */
case class SkipOperator(
    in: ExecutionOperator,
    skipDataSize: Int,
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val children: Seq[ExecutionOperator] = Seq(in)

  val outputRows: ArrayBuffer[Seq[LynxValue]] = ArrayBuffer.empty
  var isSkipDone: Boolean = false
  override def openImpl(): Unit = {
    in.open()
  }

  override def getNextImpl(): RowBatch = {
    if (!isSkipDone) {
      var skippedDataSize: Int = 0
      var dataBatch = in.getNext().batchData
      var returnData: Seq[Seq[LynxValue]] = Seq.empty
      while (dataBatch.nonEmpty && skippedDataSize <= skipDataSize) {
        val length = dataBatch.length
        val tmpLength = skippedDataSize + length
        if (tmpLength <= skipDataSize) dataBatch = in.getNext().batchData
        else {
          val offset = skipDataSize - skippedDataSize
          returnData = dataBatch.slice(offset, length)
        }
        skippedDataSize = tmpLength
      }
      isSkipDone = true
      RowBatch(returnData)
    } else in.getNext()
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = in.outputSchema()
}
