package com.tudb.blockchain.server

import scala.beans.BeanProperty
import scala.collection.JavaConverters._

/**
  *@description:
  */
class JSONTransaction(_fromAddress: String, _toAddress: String, _wei: String) {
  @BeanProperty
  val from_address: String = _fromAddress

  @BeanProperty
  val to_address: String = _toAddress

  @BeanProperty
  val wei: String = _wei
}
