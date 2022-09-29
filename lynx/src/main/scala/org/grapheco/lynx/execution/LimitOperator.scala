package org.grapheco.lynx.execution

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
    in: ExecutionOperator,
    limitDataSize: Int,
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val children: Seq[ExecutionOperator] = Seq(in)

  var isLimitDone: Boolean = false
  var countLimitNum: Int = 0

  override def openImpl(): Unit = {
    in.open()
  }

  override def getNextImpl(): RowBatch = {
    if (!isLimitDone) {
      val dataBatch = in.getNext()
      if (dataBatch.batchData.nonEmpty) {
        val currentLength = countLimitNum + dataBatch.batchData.length
        if (currentLength <= limitDataSize) {
          countLimitNum = currentLength
          dataBatch
        } else {
          isLimitDone = true
          val offset = limitDataSize - currentLength
          RowBatch(dataBatch.batchData.slice(0, offset))
        }
      } else {
        isLimitDone = true
        RowBatch(Seq.empty)
      }
    } else RowBatch(Seq.empty)
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = in.outputSchema()
}
