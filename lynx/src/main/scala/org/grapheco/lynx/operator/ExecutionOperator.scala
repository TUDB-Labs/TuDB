package org.grapheco.lynx

import org.grapheco.lynx.types.LynxValue
import org.grapheco.metrics.DomainObject
import org.opencypher.v9_0.expressions.Expression

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

  def getOperatorName(): String

  def close(): Unit = {
    closeImpl()
  }
  // to be implemented by concrete operators
  def closeImpl(): Unit

  def outputSchema(): Seq[(String, LynxType)]
}

case class RowBatch(batchData: Seq[Seq[LynxValue]]) {}
