package org.grapheco.lynx.execution

import org.grapheco.lynx.execution.utils.OperatorUtils
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, LynxType, RowBatch}
import org.opencypher.v9_0.ast.ReturnItem

/**
  *@description: This operator groups the input from `in` by keys of `aggregationItems`
  *                and evaluates aggregations by values of `aggregationItems`.
  */
case class AggregationOperator(
    in: ExecutionOperator,
    aggregationItems: Seq[ReturnItem],
    groupingItems: Seq[ReturnItem],
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val children: Seq[ExecutionOperator] = Seq(in)

  val aggregationExprs = aggregationItems.map(x => x.name -> x.expression)
  val groupingExprs = groupingItems.map(x => x.name -> x.expression)

  var schema: Seq[(String, LynxType)] = Seq.empty

  var allGroupedData: Iterator[Array[Seq[LynxValue]]] = Iterator.empty
  var hasPulledData: Boolean = false

  override def openImpl(): Unit = {
    in.open()
    schema = (aggregationExprs ++ groupingExprs).map(col =>
      col._1 -> expressionEvaluator.typeOf(col._2, in.outputSchema().toMap)
    )
  }

  override def getNextImpl(): RowBatch = {
    if (!hasPulledData) {
      val columnNames = in.outputSchema().map(f => f._1)
      val allData = OperatorUtils.getOperatorAllOutputs(in).flatMap(rowData => rowData.batchData)
      val result = if (groupingExprs.nonEmpty) {
        allData
          .map(record => {
            val recordCtx = expressionContext.withVars(columnNames.zip(record).toMap)
            groupingExprs.map(col => expressionEvaluator.eval(col._2)(recordCtx)) -> recordCtx
          })
          .toSeq // each row point to a recordCtx
          // group by record
          .groupBy(recordAndExprCtx => recordAndExprCtx._1)
          // each grouped record --> multiple recordCtx
          .mapValues(recordAndExprCtx => recordAndExprCtx.map(rc => rc._2))
          .map {
            case (groupingValue, recordCtx) =>
              groupingValue ++ {
                aggregationExprs.map {
                  case (name, expr) =>
                    expressionEvaluator.aggregateEval(expr)(recordCtx)
                }
              }
          }
      } else {
        val allRecordContext = allData.map { record =>
          expressionContext.withVars(columnNames.zip(record).toMap)
        }.toSeq
        Iterator(aggregationExprs.map {
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
