package org.grapheco.tudb.importer

import io.netty.buffer.{ByteBuf, ByteBufAllocator, Unpooled}
import org.grapheco.tudb.serializer.BaseSerializer

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 19:14 2020/12/24
  * @Modified By:
  */
// Thread safe Id manager.
class MetaIdManager(maxSize: Int) extends IdMapManager {
  override protected var MAX_SIZE: Int = maxSize
}

trait IdMapManager {

  protected var MAX_SIZE: Int
  protected var _count: AtomicInteger = new AtomicInteger(1)
  protected var _availableIdQueue: mutable.Queue[Int] =
    this.synchronized(mutable.Queue[Int]())
  protected var _id2Name: Map[Int, String] = Map[Int, String]()
  protected var _name2Id: Map[String, Int] = Map[String, Int]()

  def init(bytes: Array[Byte]): Unit = {
    val deserialized: (Int, Int, mutable.Queue[Int], Map[Int, String], Map[String, Int]) =
      deserialize(bytes)
    MAX_SIZE = deserialized._1
    _count = new AtomicInteger(deserialized._2)
    _availableIdQueue = deserialized._3
    _id2Name = deserialized._4
    _name2Id = deserialized._5
  }

  def all: Map[Int, String] = _id2Name

  def isIdUsed(id: Int) = _id2Name.contains(id)
  def isNameExists(name: String) = _name2Id.contains(name)

  def getId(name: String): Int = {
    this.synchronized {
      if (_name2Id.contains(name)) _name2Id.get(name).get
      else _addName(name)
    }
  }

  def getName(id: Int): String = {
    if (_id2Name.contains(id)) _id2Name.get(id).get
    else throw new Exception(s"The id $id does not exist.")
  }

  // note: dangerous func
  def recycleId(id: Int): Boolean = {
    this.synchronized {
      if (_id2Name.contains(id)) {
        val name: String = _id2Name(id)
        _id2Name -= (id)
        _name2Id -= (name)
      }
      _availableIdQueue.enqueue(id)
      true
    }
  }

  def recycleName(name: String): Boolean = {
    this.synchronized {
      if (_name2Id.contains(name)) {
        val id = _name2Id(name)
        recycleId(id)
      }
      true
    }
  }

  def serialized: Array[Byte] = {
    // max_size, _count, availableIdQueue, _id2Name
    val allocator: ByteBufAllocator = ByteBufAllocator.DEFAULT
    val byteBuf: ByteBuf = allocator.buffer()
    byteBuf.writeInt(MAX_SIZE)
    byteBuf.writeInt(_count.get())
    byteBuf.writeBytes(BaseSerializer.encodeArray(_availableIdQueue.toArray))
    byteBuf.writeBytes(BaseSerializer.encodePropMap(_id2Name))
    val bytes: Array[Byte] = BaseSerializer.releaseBuf(byteBuf)
    bytes
  }

  def deserialize(
      bytes: Array[Byte]
    ): (Int, Int, mutable.Queue[Int], Map[Int, String], Map[String, Int]) = {
    val byteBuf: ByteBuf = Unpooled.wrappedBuffer(bytes)
    val maxSize: Int = byteBuf.readInt()
    val count: Int = byteBuf.readInt()
    val queue: mutable.Queue[Int] =
      BaseSerializer.decodeArray(byteBuf).asInstanceOf[mutable.Queue[Int]]
    val id2Name: Map[Int, String] =
      BaseSerializer.decodePropMap(byteBuf).asInstanceOf[Map[Int, String]]
    val name2Id: Map[String, Int] = for ((id, name) <- id2Name) yield (name, id)
    (maxSize, count, queue, id2Name, name2Id)
  }

  private def _insert(id: Int, name: String): Unit = {
    _id2Name += (id -> name)
    _name2Id += (name -> id)
  }

  private def _addName(name: String): Int = {
    this.synchronized {
      if (_availableIdQueue.length > 0) {
        val id: Int = _availableIdQueue.dequeue()
        _insert(id, name)
        id
      } else {
        _insert(_count.get(), name)
        _count.getAndIncrement()
      }
    }
  }
}
