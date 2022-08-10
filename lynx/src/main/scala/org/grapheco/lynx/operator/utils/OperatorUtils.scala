package org.grapheco.lynx.operator.utils

import org.grapheco.lynx.{ExecutionOperator, RowBatch}

import scala.collection.mutable.ArrayBuffer

/**
  *@author:John117
  *@createDate:2022/8/9
  *@description:
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
