package org.grapheco.tudb.store.index

/**
 * EmptyIndexAPI
 */
class EmptyIndexServerImpl(uri: String) extends IndexServer(uri) {


  def init(uri: String) = {
    logger.info(f"none index:${uri}")
  }

  def addIndex(key: String, value: Long): Unit = {

  }

  def removeIndex(key: String, value: Long): Unit = {

  }

  def getIndexByKey(key: String): Set[Long] = {
    Set[Long]()
  }

  def hasIndex(): Boolean = false


  override def close(): Unit = {
  }
}
