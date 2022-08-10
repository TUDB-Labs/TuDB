package org.grapheco.lynx.operator

import org.grapheco.lynx.operator.utils.OperatorUtils
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, LynxType, RowBatch}

/**
  *@description: This operator is used to distinct result data. eg:data:[1,2,1,3], return distinct data: [1,2,3]
  */
case class DistinctOperator(
    in: ExecutionOperator,
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val exprEvaluator: ExpressionEvaluator = expressionEvaluator
  override val exprContext: ExpressionContext = expressionContext

  var allDistinctData: Iterator[Array[Seq[LynxValue]]] = Iterator.empty
  var isDistinct: Boolean = false

  override def openImpl(): Unit = {
    in.open()
  }

  override def getNextImpl(): RowBatch = {
    if (!isDistinct) {
      allDistinctData = OperatorUtils
        .getOperatorAllOutputs(in)
        .flatMap(rowBatch => rowBatch.batchData)
        .distinct
        .grouped(numRowsPerBatch)
      isDistinct = true
    }
    if (allDistinctData.nonEmpty) RowBatch(allDistinctData.next())
    else RowBatch(Seq.empty)
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = in.outputSchema()
}
