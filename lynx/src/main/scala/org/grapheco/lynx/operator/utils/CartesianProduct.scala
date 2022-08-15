package org.grapheco.lynx.operator.utils

import org.grapheco.lynx.{ExecutionOperator, RowBatch}
import org.grapheco.lynx.types.LynxValue

/**
  *@description: A table N rows, B table M rows, then CartesianProduct will product N * M rows data.
  */
class CartesianProduct(smallTable: ExecutionOperator, largeTable: ExecutionOperator)
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
        val joinedRecords = largeBatch.flatMap(largeRow => {
          cachedSmallTableData.map(smallRow => smallRow ++ largeRow)
        })
        RowBatch(joinedRecords)
      } else RowBatch(Seq.empty)
    } else RowBatch(Seq.empty)
  }
}
