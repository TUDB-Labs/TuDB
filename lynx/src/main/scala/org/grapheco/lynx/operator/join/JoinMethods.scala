package org.grapheco.lynx.operator.join

import org.grapheco.lynx.RowBatch

/**
  *@description:
  */
trait JoinMethods {
  def getNext(): RowBatch
}
