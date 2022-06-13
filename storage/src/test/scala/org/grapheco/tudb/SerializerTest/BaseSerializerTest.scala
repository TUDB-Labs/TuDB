package org.grapheco.tudb.SerializerTest

import io.netty.buffer.{ByteBuf, ByteBufAllocator}
import org.grapheco.lynx.types.time.LynxDate
import org.grapheco.tudb.serializer.{BaseSerializer, SerializerDataType}
import org.junit.{Assert, Test}

import java.time.LocalDate

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 2:26 下午 2022/1/25
  * @Modified By: LianxinGao
  */
class BaseSerializerTest {
  val allocator: ByteBufAllocator = ByteBufAllocator.DEFAULT

  @Test
  def testPropMap(): Unit = {
    val properties: Map[Int, Any] = Map(
      1 -> 2147483647,
      2 -> -2147483648, // int
      3 -> 9999999999L,
      4 -> -9999999999L, // long
      5 -> 2147483647.123,
      6 -> -2147483647.2333, // float
      7 -> "abcd",
      8 -> "abcde" * 20000, // string, text
      9 -> true,
      10 -> false, // boolean
      11 -> LynxDate(LocalDate.parse("2022-03-21")),
      12 -> Array("1", 2, 3.3, true) // mix
    )
    val byteBuf1: ByteBuf = allocator.heapBuffer()
    BaseSerializer.encodePropMap(properties, byteBuf1)
    val bytes = BaseSerializer.releaseBuf(byteBuf1)
    val byteBuf2: ByteBuf = allocator.heapBuffer()
    byteBuf2.writeBytes(bytes)
    val decodedResult = BaseSerializer.decodePropMap(byteBuf2)

    for (i <- 1 to 12) {
      if (i != 12) Assert.assertEquals(decodedResult(i), properties(i))
      else {
        val d = decodedResult(i).asInstanceOf[Array[Any]]
        val p = properties(i).asInstanceOf[Array[Any]]
        require(d sameElements p)
      }
    }
  }

  @Test
  def testStringArray(): Unit = {
    // not test Chinese, windows to linux, different encoding.
    val strArray: Array[String] = Array("abc", "233", "%%$$!!")
    val byteBuf1: ByteBuf = allocator.heapBuffer()
    BaseSerializer.encodeArray(strArray, byteBuf1)
    val bytes = BaseSerializer.releaseBuf(byteBuf1)
    val byteBuf2: ByteBuf = allocator.heapBuffer()
    byteBuf2.writeBytes(bytes)
    val typeFlagId = byteBuf2.readByte().toInt
    Assert.assertEquals(
      SerializerDataType.ARRAY_STRING,
      SerializerDataType(typeFlagId)
    )
    val decodedResult =
      BaseSerializer.decodeArray(byteBuf2, SerializerDataType(typeFlagId))
    Assert.assertEquals(true, decodedResult.isInstanceOf[Array[String]])
    Assert.assertTrue(
      strArray sameElements decodedResult.asInstanceOf[Array[String]]
    )
  }

  @Test
  def testIntArray(): Unit = {
    val intArray: Array[Int] = Array(-2147483648, -1, 0, 1, 2147483647)
    val byteBuf1: ByteBuf = allocator.heapBuffer()
    BaseSerializer.encodeArray(intArray, byteBuf1)
    val bytes = BaseSerializer.releaseBuf(byteBuf1)
    val byteBuf2: ByteBuf = allocator.heapBuffer()
    byteBuf2.writeBytes(bytes)
    val typeFlagId = byteBuf2.readByte().toInt
    Assert.assertEquals(
      SerializerDataType.ARRAY_INT,
      SerializerDataType(typeFlagId)
    )
    val decodedResult =
      BaseSerializer.decodeArray(byteBuf2, SerializerDataType(typeFlagId))
    Assert.assertEquals(true, decodedResult.isInstanceOf[Array[Int]])
    Assert.assertArrayEquals(intArray, decodedResult.asInstanceOf[Array[Int]])
  }

  @Test
  def testLongArray(): Unit = {
    val longArray: Array[Long] = Array(-214748364888L, -1, 0, 1, 214748364777L)
    val byteBuf1: ByteBuf = allocator.heapBuffer()
    BaseSerializer.encodeArray(longArray, byteBuf1)
    val bytes = BaseSerializer.releaseBuf(byteBuf1)
    val byteBuf2: ByteBuf = allocator.heapBuffer()
    byteBuf2.writeBytes(bytes)
    val typeFlagId = byteBuf2.readByte().toInt
    Assert.assertEquals(
      SerializerDataType.ARRAY_LONG,
      SerializerDataType(typeFlagId)
    )
    val decodedResult =
      BaseSerializer.decodeArray(byteBuf2, SerializerDataType(typeFlagId))
    Assert.assertEquals(true, decodedResult.isInstanceOf[Array[Long]])
    Assert.assertArrayEquals(longArray, decodedResult.asInstanceOf[Array[Long]])
  }

  @Test
  def testFloatArray(): Unit = {
    val floatArray: Array[Float] =
      Array(-123.3f, 0.1234567f, 1234567.8f, 23333333.0f)
    val byteBuf1: ByteBuf = allocator.heapBuffer()
    BaseSerializer.encodeArray(floatArray, byteBuf1)
    val bytes = BaseSerializer.releaseBuf(byteBuf1)
    val byteBuf2: ByteBuf = allocator.heapBuffer()
    byteBuf2.writeBytes(bytes)
    val typeFlagId = byteBuf2.readByte().toInt
    Assert.assertEquals(
      SerializerDataType.ARRAY_FLOAT,
      SerializerDataType(typeFlagId)
    )
    val decodedResult =
      BaseSerializer.decodeArray(byteBuf2, SerializerDataType(typeFlagId))
    Assert.assertEquals(true, decodedResult.isInstanceOf[Array[Float]])
    Assert.assertTrue(
      floatArray sameElements decodedResult.asInstanceOf[Array[Float]]
    )
  }

  @Test
  def testBooleanArray(): Unit = {
    val booleanArray: Array[Boolean] = Array(true, false, true)
    val byteBuf1: ByteBuf = allocator.heapBuffer()
    BaseSerializer.encodeArray(booleanArray, byteBuf1)
    val bytes = BaseSerializer.releaseBuf(byteBuf1)
    val byteBuf2: ByteBuf = allocator.heapBuffer()
    byteBuf2.writeBytes(bytes)
    val typeFlagId = byteBuf2.readByte().toInt
    Assert.assertEquals(
      SerializerDataType.ARRAY_BOOLEAN,
      SerializerDataType(typeFlagId)
    )
    val decodedResult = BaseSerializer.decodeArray(
      byteBuf2,
      SerializerDataType(typeFlagId)
    ) // miss Boolean match
    Assert.assertEquals(true, decodedResult.isInstanceOf[Array[Boolean]])
    Assert.assertTrue(
      booleanArray sameElements decodedResult.asInstanceOf[Array[Boolean]]
    )
  }

  @Test
  def testAnyArray(): Unit = {
    val anyArray: Array[Any] = Array("1", 2, 3.0, true)
    val byteBuf1: ByteBuf = allocator.heapBuffer()
    BaseSerializer.encodeArray(anyArray, byteBuf1)
    val bytes = BaseSerializer.releaseBuf(byteBuf1)
    val byteBuf2: ByteBuf = allocator.heapBuffer()
    byteBuf2.writeBytes(bytes)
    val typeFlag = byteBuf2.readByte().toInt
    Assert.assertEquals(
      SerializerDataType.ARRAY_ANY,
      SerializerDataType(typeFlag)
    )
    val decodedResult =
      BaseSerializer.decodeArray(byteBuf2, SerializerDataType(typeFlag))
    anyArray
      .zip(decodedResult)
      .foreach(pair => Assert.assertEquals(pair._1, pair._2))
  }
}
