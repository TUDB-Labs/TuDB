/** Copyright (c) 2022 PandaDB * */
package org.grapheco.tudb.store.index

import scala.collection.mutable

/** @author: huanglin
 * @createDate: 2022-06-20 17:19:08
 * @description: this is the memory index engine .data storage on memory
 */
class MemoryIndexServerImpl(params: Map[String, String]) extends IndexServer(params) {

  private val memoryIndex = new mutable.HashMap[Any, mutable.HashSet[Long]]()

  /**
   * @see [[IndexServer.init()]]
   */
  def init(params: Map[String, String]) = {
    logger.info(f"start memory index:${params}")
  }

  /**
   * @see [[IndexServer.addIndex()]]
   */
  def addIndex(key: String, value: Long): Unit = {
    if (!memoryIndex.contains(key)) {
      memoryIndex.put(key, new mutable.HashSet[Long]())
    }
    memoryIndex(key).add(value)

  }

  /**
   * @see [[IndexServer.removeIndex()]]
   */
  def removeIndex(key: String, value: Long): Unit = {
    if (memoryIndex.contains(key)) {
      memoryIndex(key).remove(value)
    }
  }

  /**
   * @see [[IndexServer.getIndexByKey()]]
   */
  def getIndexByKey(key: String): Set[Long] = {
    logger.debug(f"use index:${key}")
    memoryIndex.get(key).map(_.toSet).getOrElse(Set[Long]())
  }

  /**
   * @see [[IndexServer.hasIndex()]]
   */
  def hasIndex(): Boolean = true

  /**
   * @see [[IndexServer.close()]]
   */
  override def close(): Unit = {
    memoryIndex.clear()
  }
}
