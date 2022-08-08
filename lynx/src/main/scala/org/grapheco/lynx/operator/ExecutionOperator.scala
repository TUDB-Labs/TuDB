package org.grapheco.lynx

import org.grapheco.lynx.types.LynxValue
import org.opencypher.v9_0.expressions.Expression

import scala.collection.mutable.ArrayBuffer

/**
  * This interface is the parent of the rest of operators.
  * The execution flow for all the children operators are: open() --> getNext() --> close()
  */
trait ExecutionOperator extends TreeNode {
  override type SerialType = ExecutionOperator

  val numRowsPerBatch = 1

  val exprEvaluator: ExpressionEvaluator
  val exprContext: ExpressionContext

  def evalExpr(expr: Expression)(implicit exprContext: ExpressionContext): LynxValue =
    exprEvaluator.eval(expr)

  // prepare for processing
  def open(): Unit = {
    openImpl()
  }
  // to be implemented by concrete operators
  def openImpl(): Unit

  // empty RowBatch means the end of output
  def getNext(): RowBatch = {
    getNextImpl()
  }
  // to be implemented by concrete operators
  def getNextImpl(): RowBatch

  def close(): Unit = {
    closeImpl()
  }
  // to be implemented by concrete operators
  def closeImpl(): Unit

  def outputSchema(): Seq[(String, LynxType)]

  def getOperatorAllOutputs(operator: ExecutionOperator): Array[RowBatch] = {
    val result = ArrayBuffer[RowBatch]()
    operator.open()
    var data = operator.getNext()
    while (data.batchData.nonEmpty) {
      result.append(data)
      data = operator.getNext()
    }
    operator.close()
    result.toArray
  }
}

case class RowBatch(batchData: Seq[Seq[LynxValue]]) {}
