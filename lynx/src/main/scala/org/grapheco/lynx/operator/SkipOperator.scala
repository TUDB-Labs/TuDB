package org.grapheco.lynx.operator

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, LynxType, RowBatch}
import org.opencypher.v9_0.expressions.Expression

import scala.collection.mutable.ArrayBuffer

/**
  *@description: This operator is used to skip data. eg: [1,2,3,4], skip 2, then return [3,4]
  */
case class SkipOperator(
    skipExpr: Expression,
    in: ExecutionOperator,
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val exprEvaluator: ExpressionEvaluator = expressionEvaluator
  override val exprContext: ExpressionContext = expressionContext

  var skipLength: Int = 0
  val outputRows: ArrayBuffer[Seq[LynxValue]] = ArrayBuffer.empty
  var isSkipped: Boolean = false

  override def openImpl(): Unit = {
    in.open()
    skipLength = evalExpr(skipExpr)(exprContext).value.asInstanceOf[Number].intValue()
  }

  override def getNextImpl(): RowBatch = {
    if (!isSkipped) {
      val tmpDataArray: ArrayBuffer[Seq[LynxValue]] = ArrayBuffer.empty
      var hasNextData = true
      while (tmpDataArray.length <= skipLength && hasNextData) {
        val data = in.getNext()
        if (data.batchData.isEmpty) hasNextData = false
        tmpDataArray.append(data.batchData: _*)
      }
      isSkipped = true
      if (tmpDataArray.length <= skipLength) in.getNext()
      else RowBatch(tmpDataArray.slice(skipLength, tmpDataArray.length))
    } else in.getNext()
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = in.outputSchema()
}
