/** Copyright (c) 2022 PandaDB * */
package org.grapheco.tudb.store.index

/** @author: huagnlin
 * @createDate: 2022-06-20 17:19:08
 * @description: this is the empty index engine. all method do nothing
 * method hasIndex return false
 */
class EmptyIndexServerImpl(params: Map[String,String]) extends IndexServer(params) {


  def init(params: Map[String,String]) = {
    logger.info(f"empty index:${params}")
  }

  def addIndex(key: String, value: Long): Unit = {

  }

  def removeIndex(key: String, value: Long): Unit = {

  }

  def getIndexByKey(key: String): Set[Long] = {
    Set[Long]()
  }
  /**
   * @see  [[IndexServer.hasIndex()]]
   */
  def hasIndex(): Boolean = false


  override def close(): Unit = {
  }
}
