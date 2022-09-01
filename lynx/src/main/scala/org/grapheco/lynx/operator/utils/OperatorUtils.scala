package org.grapheco.lynx.operator.utils

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.{ExecutionOperator, RowBatch}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

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

  def getOperatorAllResultAsJavaList(
      operator: ExecutionOperator
    ): java.util.List[java.util.List[LynxValue]] = {
    getOperatorAllOutputs(operator).map(f => f.batchData.flatten).map(f => f.asJava).toList.asJava
  }
}
