// Copyright 2022 The TuDB Authors. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.grapheco.tudb.serializer

import io.netty.buffer.{ByteBuf, PooledByteBufAllocator}
import org.grapheco.lynx.types.time.LynxDate

import java.io.ByteArrayOutputStream
import java.time.LocalDate
import java.time.format.DateTimeFormatter

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 4:34 下午 2022/1/23
  * @Modified By:
  */
object BaseSerializer extends BaseSerializer {
  val allocator: PooledByteBufAllocator = PooledByteBufAllocator.DEFAULT

  def encodeString(string: String): Array[Byte] = {
    val _bytebuf = allocator.directBuffer()
    _encodeString(string, _bytebuf)
    BaseSerializer.releaseBuf(_bytebuf)
  }

  // For bytes including the type flag
  def decodeStringWithFlag(bytes: Array[Byte]): String = {
    val _bytebuf = allocator.directBuffer()
    _bytebuf.writeBytes(bytes)
    val string = _decodeStringWithFlag(_bytebuf)
    _bytebuf.release()
    string
  }

  // For bytes notincluding the type flag
  def decodeStringWithoutFlag(bytes: Array[Byte]): String = {
    val _bytebuf = allocator.directBuffer()
    _bytebuf.writeBytes(bytes)
    val string = _decodeStringWithoutFlag(_bytebuf)
    _bytebuf.release()
    string
  }

  def encodeArray(array: Array[_]): Array[Byte] = {
    val _bytebuf = allocator.directBuffer()
    BaseSerializer.encodeArray(array, _bytebuf)
    BaseSerializer.releaseBuf(_bytebuf)
  }

  def decodeArray(bytes: Array[Byte]): Array[_] = {
    val _bytebuf = allocator.directBuffer()
    _bytebuf.writeBytes(bytes)
    val typeFlag: SerializerDataType.Value = SerializerDataType(
      _bytebuf.readByte().toInt
    )
    BaseSerializer.decodeArray(_bytebuf, typeFlag)
  }

  def decodeArray(byteBuf: ByteBuf): Array[_] = {
    val typeFlag: SerializerDataType.Value = SerializerDataType(
      byteBuf.readByte().toInt
    )
    BaseSerializer.decodeArray(byteBuf, typeFlag)
  }

  def encodePropMap(map: Map[Int, _]): Array[Byte] = {
    val _bytebuf = allocator.directBuffer()
    BaseSerializer.encodePropMap(map, _bytebuf)
    BaseSerializer.releaseBuf(_bytebuf)
  }

  def decodePropMap(bytes: Array[Byte]): Map[Int, Any] = {
    val _bytebuf = allocator.directBuffer()
    _bytebuf.writeBytes(bytes)
    val props: Map[Int, Any] = BaseSerializer.decodePropMap(_bytebuf)
    _bytebuf.release()
    props
  }

  def encodeLong(long: Long): Array[Byte] = {
    val _bytebuf = allocator.directBuffer()
    _bytebuf.writeLong(long)
    BaseSerializer.releaseBuf(_bytebuf)
  }

  def decodeLong(bytes: Array[Byte], readIndex: Int = 0): Long = {
    val _bytebuf = allocator.directBuffer()
    _bytebuf.writeBytes(bytes)
    _bytebuf.readerIndex(readIndex)
    val long = _bytebuf.readLong()
    _bytebuf.release()
    long
  }

  def encodeInt(int: Int): Array[Byte] = {
    val _bytebuf = allocator.directBuffer()
    _bytebuf.writeInt(int)
    BaseSerializer.releaseBuf(_bytebuf)
  }

  def decodeInt(bytes: Array[Byte], readIndex: Int = 0): Int = {
    val _bytebuf = allocator.directBuffer()
    _bytebuf.writeBytes(bytes)
    _bytebuf.readerIndex(readIndex)
    val int = _bytebuf.readInt()
    _bytebuf.release()
    int
  }

}

/*
 * The BaseSerializer is designed for basic data format.
 * All the methods of it requires a byteBuf.
 * */
trait BaseSerializer {

  protected def _encodeString(content: String, byteBuf: ByteBuf): ByteBuf = {
    val contentInBytes: Array[Byte] = content.getBytes
    val byteLength = contentInBytes.length
    if (byteLength < 32767) {
      byteBuf.writeByte(SerializerDataType.STRING.id.toByte)
      byteBuf.writeShort(byteLength)
    } else {
      byteBuf.writeByte(SerializerDataType.TEXT.id.toByte)
      byteBuf.writeInt(byteLength)
    }
    byteBuf.writeBytes(contentInBytes)
    byteBuf
  }

  protected def _encodeDate(date: LynxDate, byteBuf: ByteBuf): ByteBuf = {
    byteBuf.writeByte(SerializerDataType.DATE.id.toByte)
    _encodeString(date.value.toString, byteBuf)
  }

  protected def _decodeDate(byteBuf: ByteBuf): LynxDate = {
    byteBuf.readByte
    val dataInStr: String = _decodeStringWithoutFlag(byteBuf)
    val fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val date = LocalDate.parse(dataInStr, fmt)
    LynxDate(date)
  }

  protected def _decodeStringWithoutFlag(byteBuf: ByteBuf): String = {
    val len: Int = byteBuf.readShort().toInt
    val bos: ByteArrayOutputStream = new ByteArrayOutputStream()
    byteBuf.readBytes(bos, len)
    bos.toString
  }

  protected def _decodeStringWithFlag(byteBuf: ByteBuf): String = {
    byteBuf.readByte() // drop the String Flag
    _decodeStringWithoutFlag(byteBuf)
  }

  protected def _decodeText(byteBuf: ByteBuf): String = {
    val len: Int = byteBuf.readInt()
    val bos: ByteArrayOutputStream = new ByteArrayOutputStream()
    byteBuf.readBytes(bos, len)
    bos.toString
  }

  def encodePropMap(map: Map[Int, _], byteBuf: ByteBuf): ByteBuf = {
    val propNum: Int = map.size
    byteBuf.writeByte(propNum)
    map.foreach(kv => _writeKV(kv._1, kv._2, byteBuf))
    byteBuf
  }

  def encodeArray(array: Array[_], byteBuf: ByteBuf): ByteBuf = {
    array match {
      case stringArr: Array[String] => {
        byteBuf.writeByte(SerializerDataType.ARRAY_STRING.id)
        val len: Int = array.length
        byteBuf.writeInt(len)
        stringArr.foreach(item => _encodeString(item, byteBuf))
      }
      case intArr: Array[Int] => {
        byteBuf.writeByte(SerializerDataType.ARRAY_INT.id)
        val len: Int = array.length
        byteBuf.writeInt(len)
        intArr.foreach(item => byteBuf.writeInt(item))
      }
      case longArr: Array[Long] => {
        byteBuf.writeByte(SerializerDataType.ARRAY_LONG.id)
        val len: Int = array.length
        byteBuf.writeInt(len)
        longArr.foreach(item => byteBuf.writeLong(item))
      }
      case doubleArr: Array[Double] => {
        byteBuf.writeByte(SerializerDataType.ARRAY_DOUBLE.id)
        val len: Int = array.length
        byteBuf.writeInt(len)
        doubleArr.foreach(item => byteBuf.writeDouble(item))
      }
      case floatArr: Array[Float] => {
        byteBuf.writeByte(SerializerDataType.ARRAY_FLOAT.id)
        val len: Int = array.length
        byteBuf.writeInt(len)
        floatArr.foreach(item => byteBuf.writeFloat(item))
      }
      case boolArr: Array[Boolean] => {
        byteBuf.writeByte(SerializerDataType.ARRAY_BOOLEAN.id)
        val len: Int = array.length
        byteBuf.writeInt(len)
        boolArr.foreach(item => byteBuf.writeBoolean(item))
      }
      case anyArr: Array[_] => {
        byteBuf.writeByte(SerializerDataType.ARRAY_ANY.id)
        val len: Int = array.length
        byteBuf.writeInt(len)
        anyArr.foreach(item => _encodeAny(item, byteBuf))
      }

    }
    byteBuf
  }

  def decodePropMap[_](byteBuf: ByteBuf): Map[Int, _] = {
    val propNum: Int = byteBuf.readByte().toInt
    val propsMap: Map[Int, _] = new Array[Int](propNum)
      .map(item => {
        val propId: Int = byteBuf.readInt()
        val propType: Int = byteBuf.readByte().toInt
        val propValue = SerializerDataType(propType) match {
          case SerializerDataType.STRING  => _decodeStringWithoutFlag(byteBuf)
          case SerializerDataType.INT     => byteBuf.readInt()
          case SerializerDataType.LONG    => byteBuf.readLong()
          case SerializerDataType.DOUBLE  => byteBuf.readDouble()
          case SerializerDataType.FLOAT   => byteBuf.readFloat()
          case SerializerDataType.BOOLEAN => byteBuf.readBoolean()
          case SerializerDataType.DATE    => _decodeDate(byteBuf)
          case SerializerDataType.TEXT    => _decodeText(byteBuf)
          case SerializerDataType.ARRAY_STRING =>
            decodeArray[String](byteBuf, SerializerDataType.ARRAY_STRING)
          case SerializerDataType.ARRAY_INT =>
            decodeArray[Int](byteBuf, SerializerDataType.ARRAY_INT)
          case SerializerDataType.ARRAY_LONG =>
            decodeArray[Long](byteBuf, SerializerDataType.ARRAY_LONG)
          case SerializerDataType.ARRAY_DOUBLE =>
            decodeArray[Double](byteBuf, SerializerDataType.ARRAY_DOUBLE)
          case SerializerDataType.ARRAY_FLOAT =>
            decodeArray[Float](byteBuf, SerializerDataType.ARRAY_FLOAT)
          case SerializerDataType.ARRAY_BOOLEAN =>
            decodeArray[Boolean](byteBuf, SerializerDataType.ARRAY_BOOLEAN)
          case SerializerDataType.ARRAY_ANY =>
            decodeArray[Any](byteBuf, SerializerDataType.ARRAY_ANY)
          case _ => throw new Exception(s"Unexpected TypeId ${propType}")
        }
        propId -> propValue
      })
      .toMap
    propsMap
  }

  def decodeArray[T](byteBuf: ByteBuf, propType: SerializerDataType.Value): Array[_] = {
    val length = byteBuf.readInt()
    propType match {
      case SerializerDataType.ARRAY_STRING =>
        new Array[String](length).map(_ => {
          // This byte is the Flag of String.
          byteBuf.readByte()
          _decodeStringWithoutFlag(byteBuf)
        })
      case SerializerDataType.ARRAY_INT =>
        new Array[Int](length).map(_ => byteBuf.readInt())
      case SerializerDataType.ARRAY_DOUBLE =>
        new Array[Double](length).map(_ => byteBuf.readDouble())
      case SerializerDataType.ARRAY_FLOAT =>
        new Array[Float](length).map(_ => byteBuf.readFloat())
      case SerializerDataType.ARRAY_LONG =>
        new Array[Long](length).map(_ => byteBuf.readLong())
      case SerializerDataType.ARRAY_BOOLEAN =>
        new Array[Boolean](length).map(_ => byteBuf.readBoolean())
      case SerializerDataType.ARRAY_ANY =>
        new Array[Any](length).map(_ => _decodeAny(byteBuf))
    }
  }

  protected def _encodeAny(value: Any, byteBuf: ByteBuf): Unit = {
    value match {
      case stringValue: String => _encodeString(stringValue, byteBuf)
      case intValue: Int => {
        byteBuf.writeByte(SerializerDataType.INT.id.toByte)
        byteBuf.writeInt(intValue)
      }
      case longValue: Long => {
        byteBuf.writeByte(SerializerDataType.LONG.id.toByte)
        byteBuf.writeLong(longValue)
      }
      case doubleValue: Double => {
        byteBuf.writeByte(SerializerDataType.DOUBLE.id.toByte)
        byteBuf.writeDouble(doubleValue)
      }
      case floatValue: Float => {
        byteBuf.writeByte(SerializerDataType.FLOAT.id.toByte)
        byteBuf.writeFloat(floatValue)
      }
      case boolValue: Boolean => {
        byteBuf.writeByte(SerializerDataType.BOOLEAN.id.toByte)
        byteBuf.writeBoolean(boolValue)
      }
    }
  }

  protected def _decodeAny[_](byteBuf: ByteBuf): Any = {
    val typeFlag = SerializerDataType(byteBuf.readByte().toInt)
    typeFlag match {
      case SerializerDataType.INT     => byteBuf.readInt()
      case SerializerDataType.LONG    => byteBuf.readLong()
      case SerializerDataType.STRING  => _decodeStringWithoutFlag(byteBuf)
      case SerializerDataType.DOUBLE  => byteBuf.readDouble()
      case SerializerDataType.FLOAT   => byteBuf.readFloat()
      case SerializerDataType.BOOLEAN => byteBuf.readBoolean()
      case _ =>
        throw new Exception(s"Unexpected type for typeId ${typeFlag.id}")
    }
  }

  protected def _writeKV(keyId: Int, value: Any, byteBuf: ByteBuf): ByteBuf = {
    byteBuf.writeInt(keyId)
    value match {
      case int: Int => {
        byteBuf.writeByte(SerializerDataType.INT.id.toByte)
        byteBuf.writeInt(int)
      }
      case double: Double => {
        byteBuf.writeByte(SerializerDataType.DOUBLE.id.toByte)
        byteBuf.writeDouble(double)
      }
      case string: String => {
        _encodeString(string, byteBuf)
      }
      case long: Long => {
        byteBuf.writeByte(SerializerDataType.LONG.id.toByte)
        byteBuf.writeLong(long)
      }
      case bool: Boolean => {
        byteBuf.writeByte(SerializerDataType.BOOLEAN.id.toByte)
        byteBuf.writeBoolean(bool)
      }
      case float: Float => {
        byteBuf.writeByte(SerializerDataType.FLOAT.id.toByte)
        byteBuf.writeFloat(float)
      }
      case date: LynxDate => {
        byteBuf.writeByte(SerializerDataType.DATE.id.toByte)
        _encodeString(date.value.toString, byteBuf)
      }
      case array: Array[_] => encodeArray(array, byteBuf)

      case _ => _encodeString(value.asInstanceOf[String], byteBuf)
    }
    byteBuf
  }

  def releaseBuf(byteBuf: ByteBuf): Array[Byte] = {
    val dst = new Array[Byte](byteBuf.writerIndex())
    byteBuf.readBytes(dst)
    byteBuf.release()
    dst
  }

  def exportBuf(byteBuf: ByteBuf): Array[Byte] = {
    val dst = new Array[Byte](byteBuf.writerIndex())
    byteBuf.readBytes(dst)
    byteBuf.clear()
    dst
  }

}
