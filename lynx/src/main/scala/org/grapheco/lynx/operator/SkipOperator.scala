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
      val remainDataArray: ArrayBuffer[Seq[LynxValue]] = ArrayBuffer.empty

      val skipBatch = skipLength / numRowsPerBatch
      val offsetNum = skipLength % numRowsPerBatch
      for (i <- 1 to skipBatch) in.getNext()
      if (offsetNum != 0)
        remainDataArray.append(in.getNext().batchData.slice(offsetNum + 1, numRowsPerBatch): _*)

      isSkipped = true
      if (remainDataArray.nonEmpty) RowBatch(remainDataArray)
      else in.getNext()
    } else in.getNext()
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = in.outputSchema()
}
