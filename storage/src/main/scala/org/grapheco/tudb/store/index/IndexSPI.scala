package org.grapheco.tudb.store.index

/** @Author: Airzihao
 * @Description:
 * @Date: Created at 9:57 下午 2022/1/25
 * @Modified By:
 */
abstract class IndexSPI(uri: String) {
  init(uri)

  def init(uri: String)

  def addIndex(key: Any, value: Long): Unit

  def removeIndex(key: Any, value: Long): Unit

  def getIndexByKey(key: Any): Set[Long]

  def hasIndex(): Boolean

  def close(): Unit

}
