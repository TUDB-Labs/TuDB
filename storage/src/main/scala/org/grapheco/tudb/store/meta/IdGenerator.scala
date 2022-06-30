package org.grapheco.tudb.store.meta

import org.grapheco.tudb.serializer.MetaDataSerializer
import org.grapheco.tudb.store.storage.KeyValueDB

import java.util.concurrent.atomic.AtomicLong

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 8:57 下午 2022/2/1
  * @Modified By:
  */
class IdGenerator(val db: KeyValueDB, val sequenceSize: Int) {

  //  val keyBytes: Array[Byte]
  private val id: AtomicLong = {
    val iter = db.newIterator()
    iter.seekToLast()
    val value: Array[Byte] = iter.key()
    if (value == null) {
      new AtomicLong(0)
    } else {
      val current: Long =
        if (value.length < 8) 0L else MetaDataSerializer.decodeCurrentId(value)
      new AtomicLong(current)
    }
  }

  private val bound = new AtomicLong(0)

  private def slideDown(current: Long): Unit = {
    val end = current + sequenceSize - 1
    //    writeId(end)
    bound.set(end)
  }

  private def slideDownIfNeeded(nid: Long): Unit = {
    if (nid > bound.get()) {
      slideDown(nid)
    }
  }

  def currentId() = id.get()

  def update(newId: Long): Unit = {
    if (newId < currentId()) {
      throw new LowerIdSetException(newId)
    }

    id.set(newId)
    slideDownIfNeeded(newId)
  }

  def nextId(): Long = {
    val nid = id.incrementAndGet()
    //all ids consumed
    slideDownIfNeeded(nid)
    nid
  }
}

class LowerIdSetException(id: Long) extends RuntimeException(s"lower id set: $id") {}
