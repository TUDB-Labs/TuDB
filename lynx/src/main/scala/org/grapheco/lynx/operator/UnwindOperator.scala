package org.grapheco.lynx.operator

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, LynxType, RowBatch}
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
    in: ExecutionOperator,
    toUnwindSchemaName: String,
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val exprEvaluator: ExpressionEvaluator = expressionEvaluator
  override val exprContext: ExpressionContext = expressionContext
  var colNames: Seq[String] = _

  override def openImpl(): Unit = {
    in.open()
    colNames = in.outputSchema().map(nameAndType => nameAndType._1)
  }

  override def getNextImpl(): RowBatch = {
    if (!colNames.contains(toUnwindSchemaName)) return RowBatch(Seq.empty)

    var batchData: Seq[Seq[LynxValue]] = Seq.empty
    var unwindResult: Seq[Seq[LynxValue]] = Seq.empty
    do {
      batchData = in.getNext().batchData
      if (batchData.isEmpty) return RowBatch(Seq.empty)
      unwindResult = batchData.flatMap(rowData => {
        val recordMap = colNames.zip(rowData).toMap
        val unwindValue = recordMap(toUnwindSchemaName) match {
          case lst: LynxList      => lst.value
          case element: LynxValue => List(element)
        }
        unwindValue.map(lynxValue => Seq(lynxValue))
      })
    } while (unwindResult.isEmpty)

    RowBatch(unwindResult)
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = Seq((toUnwindSchemaName, CTAny))
}