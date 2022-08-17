package org.grapheco.lynx.operator

import org.grapheco.lynx.operator.join.{JoinMethods, JoinType, DefaultJoin}
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, LynxType, RowBatch}
import org.opencypher.v9_0.expressions.Expression

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
  override val children: Seq[ExecutionOperator] = Seq(smallTable, largeTable)
  override val exprEvaluator: ExpressionEvaluator = expressionEvaluator
  override val exprContext: ExpressionContext = expressionContext

  var joinedTableSchema: Seq[(String, LynxType)] = _
  var joinMethods: JoinMethods = _

  override def openImpl(): Unit = {
    smallTable.open()
    largeTable.open()

    val smallCols =
      smallTable.outputSchema().map(nameAndType => nameAndType._1).zipWithIndex.toMap
    val largeCols =
      largeTable.outputSchema().map(nameAndType => nameAndType._1).zipWithIndex.toMap
    val joinCols = smallCols.keys.filter(col => largeCols.contains(col)).toSeq
    joinedTableSchema = smallTable
      .outputSchema() ++ largeTable.outputSchema().filter(x => !joinCols.contains(x._1))

    joinType match {
      case JoinType.DEFAULT =>
        joinMethods = DefaultJoin(
          smallTable,
          largeTable,
          smallCols,
          largeCols,
          joinCols,
          joinedTableSchema,
          filterExpression,
          exprEvaluator,
          exprContext
        )
    }
  }

  override def getNextImpl(): RowBatch = {
    joinMethods.getNext()
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = joinedTableSchema
}
