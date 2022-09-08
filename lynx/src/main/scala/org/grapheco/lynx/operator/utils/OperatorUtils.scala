package org.grapheco.lynx.operator.utils

import org.grapheco.lynx.{ExecutionOperator, RowBatch}

import scala.collection.mutable.ArrayBuffer

/**
  *@description: common utils for operator.
  */
object OperatorUtils {
  def getOperatorAllOutputs(operator: ExecutionOperator): Array[RowBatch] = {
    val result = ArrayBuffer[RowBatch]()
    operator.open()
    var data = operator.getNext()
    while (data.batchData.nonEmpty) {
      result.append(data)
      data = operator.getNext()
    }
    operator.close()
    result.toArray
  }
}
