package org.grapheco.tudb.store.index

/**
 * EmptyIndexAPI
 */
class EmptyIndexAPI(uri: String) extends IndexSPI(uri) {


  def init(uri: String) = {
    println(f"none index:${uri}")
  }

  def addIndex(key: Any, value: Long): Unit = {

  }

  def removeIndex(key: Any, value: Long): Unit = {

  }

  def getIndexByKey(key: Any): Set[Long] = {
    Set[Long]()
  }

  def hasIndex(): Boolean = false


  override def close(): Unit = {
  }
}
