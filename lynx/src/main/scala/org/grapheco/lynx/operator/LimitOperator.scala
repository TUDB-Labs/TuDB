package org.grapheco.lynx.operator

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, LynxType, RowBatch}
import org.opencypher.v9_0.expressions.Expression

import scala.collection.mutable.ArrayBuffer

/**
  *@description: This operator is used to limit return data. eg: [1,2,3,4], limit 2, then return [1, 2]
  */
case class LimitOperator(
    limitExpr: Expression,
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
    limitNum = evalExpr(limitExpr)(exprContext).value.asInstanceOf[Number].intValue()
  }

  override def getNextImpl(): RowBatch = {
    if (!isFinishLimit) {
      val allLimitedDataArray: ArrayBuffer[Seq[LynxValue]] = ArrayBuffer.empty
      val batchNum = limitNum / numRowsPerBatch
      val offsetNum = limitNum % numRowsPerBatch
      for (i <- 1 to batchNum) allLimitedDataArray.append(in.getNext().batchData: _*)
      if (offsetNum != 0) allLimitedDataArray.append(in.getNext().batchData.slice(0, offsetNum): _*)
      allGroupedLimitedDataArray = allLimitedDataArray.grouped(numRowsPerBatch)
      isFinishLimit = true
    }
    if (allGroupedLimitedDataArray.nonEmpty) RowBatch(allGroupedLimitedDataArray.next())
    else RowBatch(Seq.empty)
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = in.outputSchema()
}
