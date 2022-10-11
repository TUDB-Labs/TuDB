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

import java.nio.charset.{Charset, StandardCharsets}

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 16:51 2022/3/17
  * @Modified By:
  */
object ByteUtils {
  def setLong(bytes: Array[Byte], index: Int, value: Long): Unit = {
    bytes(index) = (value >>> 56).toByte
    bytes(index + 1) = (value >>> 48).toByte
    bytes(index + 2) = (value >>> 40).toByte
    bytes(index + 3) = (value >>> 32).toByte
    bytes(index + 4) = (value >>> 24).toByte
    bytes(index + 5) = (value >>> 16).toByte
    bytes(index + 6) = (value >>> 8).toByte
    bytes(index + 7) = value.toByte
  }

  def getLong(bytes: Array[Byte], index: Int): Long = {
    (bytes(index).toLong & 0xff) << 56 |
      (bytes(index + 1).toLong & 0xff) << 48 |
      (bytes(index + 2).toLong & 0xff) << 40 |
      (bytes(index + 3).toLong & 0xff) << 32 |
      (bytes(index + 4).toLong & 0xff) << 24 |
      (bytes(index + 5).toLong & 0xff) << 16 |
      (bytes(index + 6).toLong & 0xff) << 8 |
      bytes(index + 7).toLong & 0xff
  }

  def setDouble(bytes: Array[Byte], index: Int, value: Double): Unit =
    setLong(bytes, index, java.lang.Double.doubleToLongBits(value))

  def getDouble(bytes: Array[Byte], index: Int): Double =
    java.lang.Double.longBitsToDouble(getLong(bytes, index))

  def setInt(bytes: Array[Byte], index: Int, value: Int): Unit = {
    bytes(index) = (value >>> 24).toByte
    bytes(index + 1) = (value >>> 16).toByte
    bytes(index + 2) = (value >>> 8).toByte
    bytes(index + 3) = value.toByte
  }

  def getInt(bytes: Array[Byte], index: Int): Int = {
    (bytes(index) & 0xff) << 24 |
      (bytes(index + 1) & 0xff) << 16 |
      (bytes(index + 2) & 0xff) << 8 |
      bytes(index + 3) & 0xff
  }

  def setFloat(bytes: Array[Byte], index: Int, value: Float): Unit =
    setInt(bytes, index, java.lang.Float.floatToIntBits(value))

  def getFloat(bytes: Array[Byte], index: Int): Float =
    java.lang.Float.intBitsToFloat(getInt(bytes, index))

  def setShort(bytes: Array[Byte], index: Int, value: Short): Unit = {
    bytes(index) = (value >>> 8).toByte
    bytes(index + 1) = value.toByte
  }

  def getShort(bytes: Array[Byte], index: Int): Short = {
    (bytes(index) << 8 | bytes(index + 1) & 0xff).toShort
  }

  def setByte(bytes: Array[Byte], index: Int, value: Byte): Unit = {
    bytes(index) = value
  }

  def getByte(bytes: Array[Byte], index: Int): Byte = {
    bytes(index)
  }

  def setBoolean(bytes: Array[Byte], index: Int, value: Boolean): Unit = {
    bytes(index) = if (value) 1.toByte else 0.toByte
  }

  def getBoolean(bytes: Array[Byte], index: Int): Boolean = {
    bytes(index) == 1.toByte
  }

  def toBytes(value: Any): Array[Byte] = {
    val bytes: Array[Byte] = value match {
      case value: Boolean => if (value) Array[Byte](1) else Array[Byte](0)
      case value: Byte    => Array[Byte](value)
      case value: Short   => new Array[Byte](2)
      case value: Int     => new Array[Byte](4)
      case value: Long    => new Array[Byte](8)
      case value: Float   => new Array[Byte](4)
      case value: Double  => new Array[Byte](8)
      case value: String  => value.getBytes
      case _              => Array.emptyByteArray
    }
    value match {
      case value: Short  => ByteUtils.setShort(bytes, 0, value)
      case value: Int    => ByteUtils.setInt(bytes, 0, value)
      case value: Long   => ByteUtils.setLong(bytes, 0, value)
      case value: Float  => ByteUtils.setFloat(bytes, 0, value)
      case value: Double => ByteUtils.setDouble(bytes, 0, value)
    }
    bytes
  }

  def doubleToBytes(num: Double): Array[Byte] = {
    val bytes = new Array[Byte](8)
    ByteUtils.setDouble(bytes, 0, num)
    bytes
  }

  def floatToBytes(num: Float): Array[Byte] = {
    val bytes = new Array[Byte](4)
    ByteUtils.setFloat(bytes, 0, num)
    bytes
  }

  def shortToBytes(num: Short): Array[Byte] = {
    val bytes = new Array[Byte](2)
    ByteUtils.setShort(bytes, 0, num)
    bytes
  }

  def byteToBytes(num: Byte): Array[Byte] = Array[Byte](num)

  def booleanToBytes(b: Boolean): Array[Byte] =
    Array[Byte](if (b) 1.toByte else 0.toByte)

  def stringToBytes(str: String, charset: Charset = StandardCharsets.UTF_8): Array[Byte] = {
    str.getBytes(charset)
  }

  def stringFromBytes(
      bytes: Array[Byte],
      offset: Int = 0,
      length: Int = 0,
      charset: Charset = StandardCharsets.UTF_8
    ): String = {
    if (length > 0) new String(bytes, offset, length, charset)
    else new String(bytes, offset, bytes.length, charset)
  }

}
