package org.grapheco.lynx.operator.join

import org.grapheco.lynx.operator.utils.OperatorUtils
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.{LynxBoolean, LynxNull}
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, RowBatch}
import org.opencypher.v9_0.expressions.Expression

/**
  *@description: When two table have properties reference, valueHashJoin is required.
  *    like:
  *         match (n:Person)
  *         match (m:City)
  *         where m.name=n.city
  *         return n,m
  */
class ValueHashJoin(
    smallTable: ExecutionOperator,
    largeTable: ExecutionOperator,
    filterExpression: Seq[Expression],
    schema: Seq[String],
    exprEvaluator: ExpressionEvaluator,
    exprContext: ExpressionContext)
  extends JoinMethods {

  var cachedSmallTableData: Array[Seq[LynxValue]] = Array.empty
  var isInit: Boolean = false

  override def getNext(): RowBatch = {
    if (!isInit) {
      cachedSmallTableData =
        OperatorUtils.getOperatorAllOutputs(smallTable).flatMap(batch => batch.batchData)
      isInit = true
    }
    if (cachedSmallTableData.nonEmpty) {
      val largeBatch = largeTable.getNext().batchData
      if (largeBatch.nonEmpty) {
        var joinedRecords = largeBatch.flatMap(largeRow => {
          cachedSmallTableData.map(smallRow => smallRow ++ largeRow)
        })
        filterExpression.foreach(filterExpr => {
          joinedRecords = joinedRecords.filter(record => {
            exprEvaluator.eval(filterExpr)(exprContext.withVars(schema.zip(record).toMap)) match {
              case LynxBoolean(v) => v
              case LynxNull       => false
            }
          })
        })
        if (joinedRecords.isEmpty) getNext()
        else RowBatch(joinedRecords)
      } else RowBatch(Seq.empty)
    } else RowBatch(Seq.empty)
  }
}
