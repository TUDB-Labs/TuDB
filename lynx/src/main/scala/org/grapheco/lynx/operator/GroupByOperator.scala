package org.grapheco.lynx.operator

import org.grapheco.lynx.operator.utils.OperatorUtils
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, LynxType, RowBatch}
import org.opencypher.v9_0.ast.ReturnItem

/**
  *@description: This operator is used to group data by specified expressions.
  */
case class GroupByOperator(
    aggregationItems: Seq[ReturnItem],
    groupingItems: Seq[ReturnItem],
    in: ExecutionOperator,
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val exprEvaluator: ExpressionEvaluator = expressionEvaluator
  override val exprContext: ExpressionContext = expressionContext

  val aggregationExpr = aggregationItems.map(x => x.name -> x.expression)
  val groupingExpr = groupingItems.map(x => x.name -> x.expression)

  var schema: Seq[(String, LynxType)] = Seq.empty

  var allGroupedData: Iterator[Array[Seq[LynxValue]]] = Iterator.empty
  var hasPulledData: Boolean = false

  override def openImpl(): Unit = {
    in.open()
    schema = (aggregationExpr ++ groupingExpr).map(col =>
      col._1 -> expressionEvaluator.typeOf(col._2, in.outputSchema().toMap)
    )
  }

  override def getNextImpl(): RowBatch = {
    if (!hasPulledData) {
      val columnNames = in.outputSchema().map(f => f._1)
      val allData = OperatorUtils.getOperatorAllOutputs(in).flatMap(rowData => rowData.batchData)
      val result = if (groupingExpr.nonEmpty) {
        allData
          .map(record => {
            val recordCtx = exprContext.withVars(columnNames.zip(record).toMap)
            groupingExpr.map(col => evalExpr(col._2)(recordCtx)) -> recordCtx
          })
          .toSeq // each row point to a recordCtx
          // group by record
          .groupBy(recordAndEc => recordAndEc._1)
          // each grouped record --> multiple recordCtx
          .mapValues(recordAndEc => recordAndEc.map(rc => rc._2))
          .map {
            case (groupingValue, recordCtx) =>
              groupingValue ++ {
                aggregationExpr.map {
                  case (name, expr) =>
                    expressionEvaluator.aggregateEval(expr)(recordCtx)
                }
              }
          }
      } else {
        val allRecordContext = allData.map { record =>
          expressionContext.withVars(columnNames.zip(record).toMap)
        }.toSeq
        Iterator(aggregationExpr.map {
          case (name, expression) =>
            expressionEvaluator.aggregateEval(expression)(allRecordContext)
        })
      }
      allGroupedData = result.toArray.grouped(numRowsPerBatch)
      hasPulledData = true
    }

    if (allGroupedData.nonEmpty) RowBatch(allGroupedData.next())
    else RowBatch(Seq.empty)
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = schema
}
