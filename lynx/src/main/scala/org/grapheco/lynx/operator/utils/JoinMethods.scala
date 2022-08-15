package org.grapheco.lynx.operator.utils

import org.grapheco.lynx.RowBatch

/**
  *@description:
  */
trait JoinMethods {
  def getNext(): RowBatch
}
