package org.grapheco.tudb.serializer

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 16:15 2021/4/25
  * @Modified By:
  */
object SerializerDataType extends Enumeration {
  type DataType = Value

  val STRING = Value(1, "String")
  val ARRAY_STRING = Value(-STRING.id, "Array_String")

  val INT = Value(2, "Int")
  val ARRAY_INT = Value(-INT.id, "Array_Int")

  val LONG = Value(3, "Long")
  val ARRAY_LONG = Value(-LONG.id, "Array_Long")

  val DOUBLE = Value(4, "Double")
  val ARRAY_DOUBLE = Value(-DOUBLE.id, "Array_Double")

  val FLOAT = Value(5, "Float")
  val ARRAY_FLOAT = Value(-FLOAT.id, "Array_Float")

  val BOOLEAN = Value(6, "Boolean")
  val ARRAY_BOOLEAN = Value(-BOOLEAN.id, "Array_Boolean")

  val BLOB = Value(7, "Blob")
  val ARRAY_BLOB = Value(-BLOB.id, "Array_Blob")

  val DATE = Value(8, "Date")
  val ARRAY_DATE = Value(-DATE.id, "Array_Date")

  val DATE_TIME = Value(9, "DateTime")
  val ARRAY_DATE_TIME = Value(-DATE_TIME.id, "Array_Date_Time")

  val LOCAL_DATE_TIME = Value(10, "LocalDateTime")
  val ARRAY_LOCAL_DATE_TIME = Value(-LOCAL_DATE_TIME.id, "Array_LocalDateTime")

  val LOCAL_TIME = Value(11, "LocalTime")
  val ARRAY_LOCAL_TIME = Value(-LOCAL_TIME.id, "Array_LocalTime")

  val TIME = Value(12, "Time")
  val ARRAY_TIME = Value(-TIME.id, "Time")

  // text means long string over 32667 bytes.
  val TEXT = Value(13, "Text")
  val ARRAY_TEXT = Value(-TEXT.id, "Array_Text")

  val BLOBPOINTER = Value(14, "BlobPointer")
  val ARRAY_BLOBPOINTER = Value(-BLOBPOINTER.id, "Array_BlobPointer")

  val ANY = Value(127, "Any")
  val ARRAY_ANY = Value(-ANY.id, "Array_Any")

}
