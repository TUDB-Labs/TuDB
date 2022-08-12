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

    val evalValue = evalExpr(limitExpr)(exprContext)
    evalValue match {
      case n: LynxNumber => {
        val num = n.value.asInstanceOf[Number].intValue()
        if (num >= 0) limitNum = num
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
