/** Copyright (c) 2022 PandaDB * */
package org.grapheco.tudb.store.index

import com.typesafe.scalalogging.LazyLogging
import org.grapheco.lynx.types.time.LynxDate

/** @author: huanglin
  * @createDate: 2022-06-20 17:19:08
  * @description: this is the index engine  interface.
  * memory://{any}  use hashmap storage index data
  * es://ip:port   use  es (Elasticsearch) storage index data,ip:port is es service address
  * db://{path}  use rocksdb storage index data ,path is rocksdb data storage location
  * empty is empty implement ,  use this engine where no  index is used
  */
abstract class IndexServer(params: Map[String,String]) extends LazyLogging {
  init(params)

  /** initialization index engine
    * @param uri
    */
  def init(params: Map[String,String])

  /** add one index records to index engine
    * @param key
    * @param value
    */
  def addIndex(key: String, value: Long): Unit

  /** remove on index records
    * @param key
    * @param value
    */
  def removeIndex(key: String, value: Long): Unit

  /** batch add index records
    * @param key
    * @return
    */
  def batchAddIndex(key: String, value: Set[Long]): Unit

  /** get index by key
    * @param key
    * @return
    */
  def getIndexByKey(key: String): Set[Long]

  /** return  index engine has working
    * @return
    */
  def hasIndex(): Boolean

  /** close index engine where system close
    */
  def close(): Unit

  /** encode tudb Key to string
    * @param keyType
    * @param key
    * @return
    */
  def encodeKey(keyType: Int, key: Any) = {
    val keyStr = getKeyString(key)
    f"""${keyType}_${keyStr}"""
  }

  /** get string by any value
    * @param value
    * @return
    */
  def getKeyString(value: Any) = value match {
    case (Int | Long | Float | Double | Boolean) => value.toString
    case date: LynxDate                          => date.value.toString
    case array: Array[_]                         => array.mkString("_")
    case _                                       => value.toString
  }
}
