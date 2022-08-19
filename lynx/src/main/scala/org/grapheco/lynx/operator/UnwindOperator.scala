package org.grapheco.lynx.operator

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, LynxType, RowBatch}
import org.opencypher.v9_0.expressions.{Expression, Variable}
import org.opencypher.v9_0.util.symbols.CTAny

/**
  *@description: This operator is used to unwind list:
  *                 unwind [1,2,3,4] as x return x will get 4 rows
  *                    ==>     1
  *                            2
  *                            3
  *                            4
  *                  unwind [[1,2,3], [4,5,6]] as x will get 2 rows:
  *                    ==>
  *                        [1,2,3]
  *                        [4,5,6]
  */
case class UnwindOperator(
    in: Option[ExecutionOperator],
    variable: Variable,
    expression: Expression,
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val exprEvaluator: ExpressionEvaluator = expressionEvaluator
  override val exprContext: ExpressionContext = expressionContext
  var schema: Seq[(String, LynxType)] = _
  var colNames: Seq[String] = _
  val isInDefined: Boolean = in.isDefined
  var isUnwindDone: Boolean = false

  override def openImpl(): Unit = {
    if (isInDefined) {
      in.get.open()
      schema = in.get.outputSchema() ++ Seq((variable.name, CTAny))
    } else schema = Seq((variable.name, CTAny))

    colNames = schema.map(nameAndType => nameAndType._1)
  }

  override def getNextImpl(): RowBatch = {
    if (isInDefined) {
      val batchData = in.get.getNext().batchData
      if (batchData.nonEmpty) {
        val res = batchData.flatMap(rowData => {
          val recordCtx = expressionContext.withVars(colNames.zip(rowData).toMap)
          val unwindValue = expressionEvaluator.eval(expression)(recordCtx) match {
            case lst: LynxList      => lst.value
            case element: LynxValue => List(element)
          }
          unwindValue.map(value => rowData :+ value)
        })
        if (res.nonEmpty) RowBatch(res)
        else getNextImpl()
      } else RowBatch(Seq.empty)
    }
    // just unwind, no in operator
    else {
      if (!isUnwindDone) {
        val dataSource = expressionEvaluator.eval(expression)(expressionContext) match {
          case lst: LynxList      => lst.value
          case element: LynxValue => List(element)
        }
        isUnwindDone = true
        RowBatch(dataSource.map(v => Seq(v)))
      } else RowBatch(Seq.empty)
    }
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = schema
}
