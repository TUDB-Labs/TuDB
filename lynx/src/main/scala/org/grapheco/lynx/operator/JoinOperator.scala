package org.grapheco.lynx.operator

import org.grapheco.lynx.operator.join.{CartesianProduct, JoinMethods, JoinType}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, LynxType, RowBatch}
import org.opencypher.v9_0.expressions.Expression

import scala.collection.mutable.ArrayBuffer

/**
  *@description: This operator is used to join two operators.
  *              But we don't process the situation: two operators have some same schema,
  *              this kind of join will be optimized to expand.
  */
case class JoinOperator(
    smallTable: ExecutionOperator,
    largeTable: ExecutionOperator,
    joinType: JoinType,
    filterExpression: Seq[Expression],
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val exprEvaluator: ExpressionEvaluator = expressionEvaluator
  override val exprContext: ExpressionContext = expressionContext

  private var joinedSchema: Seq[(String, LynxType)] = Seq.empty
  private var joinMethods: JoinMethods = _

  override def openImpl(): Unit = {
    smallTable.open()
    largeTable.open()
    joinedSchema = smallTable.outputSchema() ++ largeTable.outputSchema()

    joinType match {
      case JoinType.CartesianProduct => {
        joinMethods = new CartesianProduct(smallTable, largeTable)
      }
      case JoinType.ValueHashJoin => {
        ???
      }
    }
  }

  override def getNextImpl(): RowBatch = {
    joinMethods.getNext()
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = joinedSchema
}
