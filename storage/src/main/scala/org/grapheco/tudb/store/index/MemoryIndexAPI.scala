package org.grapheco.tudb.store.index

import scala.collection.mutable

/**
 * MemoryIndexAPI is memory Hashmap
 */
class MemoryIndexAPI(uri: String) extends IndexSPI(uri) {

  private val memoryIndex = new mutable.HashMap[Any, mutable.HashSet[Long]]()

  def init(uri: String) = {
    println(f"start memory index:${uri}")
  }

  def addIndex(key: Any, value: Long): Unit = {
    if (!memoryIndex.contains(key)) {
      memoryIndex.put(key, new mutable.HashSet[Long]())
    }
    memoryIndex(key).add(value)

  }

  def removeIndex(key: Any, value: Long): Unit = {
    if (memoryIndex.contains(key)) {
      memoryIndex(key).remove(value)
    }
  }

  def getIndexByKey(key: Any): Set[Long] = {
    memoryIndex.get(key).map(_.toSet).getOrElse(Set[Long]())
  }

  def hasIndex(): Boolean = true


  override def close(): Unit = {
    memoryIndex.clear()
  }
}
