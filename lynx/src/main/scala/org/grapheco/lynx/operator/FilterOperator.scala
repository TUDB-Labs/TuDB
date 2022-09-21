package org.grapheco.lynx.operator

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.{LynxBoolean, LynxNull, LynxString}
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, LynxType, RowBatch}
import org.opencypher.v9_0.expressions.Expression

import scala.collection.mutable.ArrayBuffer

/**
  *@author:John117
  *@createDate:2022/8/1
  *@description: Filter operator is used to get data that contains a specific pattern.
  */
case class FilterOperator(
    filterExpr: Expression,
    in: ExecutionOperator,
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val children: Seq[ExecutionOperator] = Seq(in)
  override val exprEvaluator: ExpressionEvaluator = expressionEvaluator
  override val exprContext: ExpressionContext = expressionContext

  var columnNames: Seq[String] = Seq.empty
  val outputRows: ArrayBuffer[Seq[LynxValue]] = ArrayBuffer()

  override def openImpl(): Unit = {
    in.open()
    columnNames = in.outputSchema().map(nameAndType => nameAndType._1)
  }

  override def getNextImpl(): RowBatch = {
    while (outputRows.length < numRowsPerBatch) {
      val inputRows = in.getNext()
      if (inputRows.batchData.isEmpty) {
        if (outputRows.nonEmpty) {
          val remainingData = outputRows.toArray.toSeq
          outputRows.clear()
          return RowBatch(remainingData)
        } else return RowBatch(Seq.empty)
      }
      inputRows.batchData.foreach(inputRow => {
        evalExpr(filterExpr)(exprContext.withVars(columnNames.zip(inputRow).toMap)) match {
          case LynxBoolean(passFilter) => if (passFilter) outputRows.append(inputRow)
          case LynxNull                => {}
        }
      })
    }
    val output = RowBatch(outputRows.toArray.toSeq)
    outputRows.clear()
    output
  }

  override def closeImpl(): Unit = {}

  override def getOperatorName(): String = "Filter"

  override def outputSchema(): Seq[(String, LynxType)] = in.outputSchema()
}
