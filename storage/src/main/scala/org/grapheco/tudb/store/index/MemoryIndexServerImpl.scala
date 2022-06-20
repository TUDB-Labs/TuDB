package org.grapheco.tudb.store.index

import scala.collection.mutable

/**
 * MemoryIndexAPI is memory Hashmap
 */
class MemoryIndexServerImpl(uri: String) extends IndexServer(uri) {

  private val memoryIndex = new mutable.HashMap[Any, mutable.HashSet[Long]]()

  def init(uri: String) = {
    logger.info(f"start memory index:${uri}")
  }

  def addIndex(key: String, value: Long): Unit = {
    if (!memoryIndex.contains(key)) {
      memoryIndex.put(key, new mutable.HashSet[Long]())
    }
    memoryIndex(key).add(value)

  }

  def removeIndex(key: String, value: Long): Unit = {
    if (memoryIndex.contains(key)) {
      memoryIndex(key).remove(value)
    }
  }

  def getIndexByKey(key: String): Set[Long] = {
    logger.debug(f"use index:${key}")
    memoryIndex.get(key).map(_.toSet).getOrElse(Set[Long]())
  }

  def hasIndex(): Boolean = true


  override def close(): Unit = {
    memoryIndex.clear()
  }
}
