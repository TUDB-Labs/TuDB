package org.grapheco.tudb.store.index

import org.grapheco.tudb.serializer.{BaseSerializer, ByteUtils}

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 17:08 2022/4/24
  * @Modified By:
  */
object IndexEncoder {

  val NULL: Byte = -1
  val FALSE_CODE: Byte = 0
  val TRUE_CODE: Byte = 1
  val INTEGER_CODE: Byte = 2
  val FLOAT_CODE: Byte = 3
  val STRING_CODE: Byte = 5
  val MAP_CODE: Byte = 6

  /** support dataType : boolean, byte, short, int, float, long, double, string
    * ╔═══════════════╦══════════════╦══════════════╗
    * ║    dataType   ║  storageType ║    typeCode  ║
    * ╠═══════════════╬══════════════╬══════════════╣
    * ║      Null     ║    Empty     ║      -1      ║  ==> TODO
    * ╠═══════════════╬══════════════╬══════════════╣
    * ║      False    ║    Empty     ║      0       ║
    * ╠═══════════════╬══════════════╬══════════════╣
    * ║      True     ║    Empty     ║      1       ║
    * ╠═══════════════╬══════════════╬══════════════╣
    * ║      Byte     ║    *Long*    ║      2       ║
    * ╠═══════════════╬══════════════╬══════════════╣
    * ║      Short    ║    *Long*    ║      2       ║
    * ╠═══════════════╬══════════════╬══════════════╣
    * ║      Int      ║    *Long*    ║      2       ║
    * ╠═══════════════╬══════════════╬══════════════╣
    * ║      Float    ║   *Double*   ║      3       ║
    * ╠═══════════════╬══════════════╬══════════════╣
    * ║     Double    ║    double    ║      3       ║
    * ╠═══════════════╬══════════════╬══════════════╣
    * ║      Long     ║     Long     ║      2       ║
    * ╠═══════════════╬══════════════╬══════════════╣
    * ║     String    ║    String    ║      5       ║
    * ╠═══════════════╬══════════════╬══════════════╣
    * ║       Map     ║  ByteArray   ║      6       ║
    * ╚═══════════════╩══════════════╩══════════════╝
    *
    * @param data the data to encode
    * @return the bytes array after encode
    */
  def encode(data: Any): Array[Byte] = {
    data match {
      case data: Boolean => Array.emptyByteArray
      case data: Byte    => integerEncode(data.toLong)
      case data: Short   => integerEncode(data.toLong)
      case data: Int     => integerEncode(data.toLong)
      case data: Float   => floatEncode(data.toDouble)
      case data: Long    => integerEncode(data)
      case data: Double  => floatEncode(data)
      case data: String  => stringEncode(data)
      // Warning: Type erasure.
      case data: Map[Int, _] => BaseSerializer.encodePropMap(data)
      case data: Any =>
        throw new Exception(
          s"this value type: ${data.getClass} is not supported"
        )
      case _ => Array.emptyByteArray
    }
  }

  def typeCode(data: Any): Byte = {
    data match {
      case data: Boolean if data  => TRUE_CODE
      case data: Boolean if !data => FALSE_CODE
      case data: Byte             => INTEGER_CODE
      case data: Short            => INTEGER_CODE
      case data: Int              => INTEGER_CODE
      case data: Float            => FLOAT_CODE
      case data: Long             => INTEGER_CODE
      case data: Double           => FLOAT_CODE
      case data: String           => STRING_CODE
      //Warning: type erasure
      case data: Map[Int, _] => MAP_CODE
      case data: Any =>
        throw new Exception(
          s"this value type: ${data.getClass} is not supported"
        )
      case _ => NULL
    }
  }

  private def integerEncode(int: Long): Array[Byte] = {
    ByteUtils.toBytes(int ^ Long.MinValue)
  }

  /** if float greater than or equal 0, the highest bit set to 1
    * else NOT each bit
    */
  private def floatEncode(float: Double): Array[Byte] = {
    val buf = ByteUtils.doubleToBytes(float)
    if (float >= 0) {
      // 0xxxxxxx => 1xxxxxxx
      ByteUtils.setInt(buf, 0, ByteUtils.getInt(buf, 0) ^ Int.MinValue)
    } else {
      // ~
      ByteUtils.setLong(buf, 0, ~ByteUtils.getLong(buf, 0))
    }
    buf
  }

  /** The string is divided into groups according to 8 bytes.
    * The last group is less than 8 bytes, and several zeros bytes are supplemented.
    * Add a byte to the end of each group. The value of the byte is 255 minus the number of 0 bytes filled by the group.
    * eg:
    * []                   =>  [0,0,0,0,0,0,0,0,247]
    * [1,2,3]              =>  [1,2,3,0,0,0,0,0,250]
    * [1,2,3,0]            =>  [1,2,3,0,0,0,0,0,251]
    * [1,2,3,4,5,6,7,8]    =>  [1,2,3,4,5,6,7,8,255,0,0,0,0,0,0,0,0,247]
    * [1,2,3,4,5,6,7,8,9]  =>  [1,2,3,4,5,6,7,8,255,9,0,0,0,0,0,0,0,248]
    */
  private def stringEncode(string: String): Array[Byte] = {
    val buf = ByteUtils.stringToBytes(string)
    val group = buf.length / 8 + 1
    val res = new Array[Byte](group * 9)
    // set value Bytes
    for (i <- buf.indices)
      res(i + i / 8) = buf(i)
    // set length Bytes
    for (i <- 1 until group)
      res(9 * i - 1) = 255.toByte
    // set last Bytes
    res(res.length - 1) = (247 + buf.length % 8).toByte
    res
  }
}
