package com.tudb.blockchain.server

import scala.beans.BeanProperty
import scala.collection.JavaConverters._

/**
  *@description:
  */
class JSONAddress(address: Seq[String]) {
  @BeanProperty
  val addressList: java.util.List[String] = seqAsJavaList(address)
}
