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
    val evalValue = evalExpr(skipExpr)(exprContext)
    evalValue match {
      case n: LynxNumber => {
        val num = n.value.asInstanceOf[Number].intValue()
        if (num >= 0) skipLength = num
        else
          throw new TuDBException(
            TuDBError.LYNX_WRONG_NUMBER_OF_ARGUMENT,
            s"Invalid input '$num', must be a positive integer."
          )
      }
      case n =>
        throw new TuDBException(
          TuDBError.LYNX_WRONG_NUMBER_OF_ARGUMENT,
          s"Invalid input '${n.value}', must be a positive integer."
        )
    }
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
