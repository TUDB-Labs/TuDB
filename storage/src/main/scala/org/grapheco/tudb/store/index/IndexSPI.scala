package org.grapheco.tudb.store.index

import com.typesafe.scalalogging.LazyLogging
import org.grapheco.lynx.types.time.LynxDate
import org.grapheco.tudb.serializer.BaseSerializer

/**
 */
abstract class IndexSPI(uri: String) extends  LazyLogging {
  init(uri)

  def init(uri: String)

  def addIndex(key: String, value: Long): Unit

  def removeIndex(key: String, value: Long): Unit

  def getIndexByKey(key: String): Set[Long]

  def hasIndex(): Boolean

  def close(): Unit

  def encodeKey(keyType:Int,key:Any)={
    val keyStr=getKeyString(key)
    f"""${keyType}_${keyStr}"""
  }

  def getKeyString(value: Any)={
    value match {
      case (Int|Long|Float|Double|Boolean) =>value.toString
      case data:LynxDate=>data.value.toString
      case array: Array[_]=>array.mkString("_")
      case _=>value.toString
    }
  }


}
