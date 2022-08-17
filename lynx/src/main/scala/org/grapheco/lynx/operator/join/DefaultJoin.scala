package org.grapheco.lynx.operator.join

import org.grapheco.lynx.operator.utils.OperatorUtils
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.{LynxBoolean, LynxNull}
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, LynxType, RowBatch}
import org.opencypher.v9_0.expressions.{Expression, Property}

/**
  *@description: When two table have properties reference, valueHashJoin is required.
  *    like:
  *         match (n:Person)
  *         match (m:City)
  *         where m.name=n.city
  *         return n,m
  */
case class DefaultJoin(
    smallTable: ExecutionOperator,
    largeTable: ExecutionOperator,
    smallCols: Map[String, Int],
    largeCols: Map[String, Int],
    joinCols: Seq[String],
    joinedTableSchema: Seq[(String, LynxType)],
    filterExprs: Seq[Expression],
    exprEvaluator: ExpressionEvaluator,
    exprContext: ExpressionContext)
  extends JoinMethods {

  var isInit: Boolean = false
  var cachedSmallTableMap: Map[Seq[LynxValue], Seq[Seq[LynxValue]]] = _
  val largeColsWithoutJoinCols = largeCols -- joinCols

  def getNext(): RowBatch = {
    if (!isInit) {
      cachedSmallTableMap = OperatorUtils
        .getOperatorAllOutputs(smallTable)
        .flatMap(batch => batch.batchData)
        .map(row => {
          val value = joinCols.map(col => row(smallCols(col)))
          value -> row
        })
        .groupBy(valueRow => valueRow._1)
        .map(kv => kv._1 -> kv._2.map(f => f._2).toSeq) // joinCols --> rows
      isInit = true
    }
    val largeBatchData = largeTable.getNext().batchData
    if (largeBatchData.nonEmpty) {
      var joinedRecords = largeBatchData.flatMap(row => {
        val largeJoinedColeValue = joinCols.map(col => row(largeCols(col)))
        cachedSmallTableMap
          .getOrElse(largeJoinedColeValue, Seq.empty)
          .map(smallRow => {
            val largeRow =
              largeColsWithoutJoinCols.toSeq
                .sortBy(f => f._2)
                .map(colAndIndex => row(colAndIndex._2))
            smallRow ++ largeRow
          })
      })
      filterExprs.foreach(expr => {
        joinedRecords = joinedRecords.filter(row => {
          exprEvaluator
            .eval(expr)(exprContext.withVars(joinedTableSchema.map(f => f._1).zip(row).toMap)) match {
            case LynxBoolean(v) => v
            case LynxNull       => false
          }
        })
      })
      if (joinedRecords.isEmpty) getNext()
      else RowBatch(joinedRecords)
    } else RowBatch(Seq.empty)
  }
}
