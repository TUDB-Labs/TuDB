package org.grapheco.lynx.operator

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxNull
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, GraphModel, LynxType, RowBatch}
import org.opencypher.v9_0.ast.{AscSortItem, DescSortItem, SortItem}
import org.opencypher.v9_0.expressions.Expression
import org.opencypher.v9_0.util.symbols.CypherType

import scala.collection.mutable.ArrayBuffer

/**
  *@author:John117
  *@createDate:2022/8/4
  *@description:
  */
case class OrderByOperator(
    sortItem: Seq[SortItem],
    in: ExecutionOperator,
    graphModel: GraphModel,
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val children: Seq[ExecutionOperator] = Seq(in)
  override val exprEvaluator: ExpressionEvaluator = expressionEvaluator
  override val exprContext: ExpressionContext = expressionContext

  var sortItems: Seq[(Expression, OrderByType)] = sortItem.map {
    case AscSortItem(expression)  => (expression, OrderByType.ASC)
    case DescSortItem(expression) => (expression, OrderByType.DESC)
  }
  var allGroupedSortedData: Iterator[Array[Seq[LynxValue]]] = Iterator.empty

  override def openImpl(): Unit = {
    in.open()
    val allData = getOperatorAllOutputs(in).flatMap(rowData => rowData.batchData)
    val schemaWithIndex = in.outputSchema().zipWithIndex.map(x => x._1._1 -> (x._1._2, x._2)).toMap
    allGroupedSortedData = allData
      .sortWith((a, b) => sortByItem(a, b, sortItems, schemaWithIndex)) // maybe it's a bad sort method.
      .toArray
      .grouped(numRowsPerBatch)
  }

  override def getNextImpl(): RowBatch = {
    if (allGroupedSortedData.nonEmpty) RowBatch(allGroupedSortedData.next())
    else RowBatch(Seq.empty)
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = in.outputSchema()

  private def sortByItem(
      a: Seq[LynxValue],
      b: Seq[LynxValue],
      items: Seq[(Expression, OrderByType)],
      schema: Map[String, (CypherType, Int)]
    ): Boolean = {
    // [true,true] means current expression cannot sort the two data.
    // The first true/false means the compare result(.sortWith is a asc method, true means the two data is asc).
    // The second true/false determines whether the next expression is required. (true means require).
    val comparedResult = items.foldLeft((true, true)) { (result, item) =>
      {
        result match {
          case (true, true) => { // means previous expression cannot compare two value
            val ev1 = expressionEvaluator.eval(item._1)(
              expressionContext.withVars(schema.toSeq.sortBy(f => f._2._2).map(_._1).zip(a).toMap)
            )
            val ev2 = expressionEvaluator.eval(item._1)(
              expressionContext.withVars(schema.toSeq.sortBy(f => f._2._2).map(_._1).zip(b).toMap)
            )
            item._2 match {
              // LynxNull = MAX
              case OrderByType.ASC => {
                if (ev1 == LynxNull && ev2 != LynxNull) (false, false)
                else if (ev1 == LynxNull && ev2 == LynxNull) (true, true)
                else if (ev1 != LynxNull && ev2 == LynxNull) (true, false)
                else (ev1 <= ev2, ev1 == ev2)
              }
              case OrderByType.DESC => {
                if (ev1 == LynxNull && ev2 != LynxNull) (true, false)
                else if (ev1 == LynxNull && ev2 == LynxNull) (true, true)
                else if (ev1 != LynxNull && ev2 == LynxNull) (false, false)
                else (ev1 >= ev2, ev1 == ev2)
              }
            }
          }
          case (true, false)  => (true, false)
          case (false, true)  => (false, true)
          case (false, false) => (false, false)
        }
      }
    }
    comparedResult._1
  }
}

trait OrderByType {}

case object OrderByType {
  case object ASC extends OrderByType
  case object DESC extends OrderByType
}
