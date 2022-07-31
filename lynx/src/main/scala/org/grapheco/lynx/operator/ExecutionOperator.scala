package org.grapheco.lynx

import org.grapheco.lynx.types.LynxValue
import org.opencypher.v9_0.ast.ReturnItem
import org.opencypher.v9_0.expressions.Expression
import scala.collection.mutable

/**
  *@author:John117
  *@createDate:2022/7/29
  *@description:
  */
trait ExecutionOperator extends TreeNode {
  override type SerialType = ExecutionOperator

  val dataBatchSize = 1

  val expressionEvaluator: ExpressionEvaluator
  val ec: ExpressionContext

  def eval(expr: Expression)(implicit ec: ExpressionContext): LynxValue =
    expressionEvaluator.eval(expr)

  // prepare for processing
  def open(): Unit = {
    openImpl()
  }
  // impl by concrete operators
  def openImpl(): Unit

  def hasNext(): Boolean

  // empty RowBatch means the end of output
  def getNext(): RowBatch = {
    getNextImpl()
  }
  // impl by concrete operators
  def getNextImpl(): RowBatch

  def close(): Unit = {
    closeImpl()
  }
  def closeImpl(): Unit

  def outputSchema(): Seq[(String, LynxType)]
}

case class RowBatch(batchData: Seq[Seq[LynxValue]]) {}
