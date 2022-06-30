package org.grapheco.tudb.store.index

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxString
import org.grapheco.tudb.store.meta.TypeManager.NodeId

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created at 11:13 2022/5/26
  * @Modified By:
  */
trait FullTextPropertyIndexStore {
  // A search task in the full-text property index do not need to specify the prop id.

  def getIdsByProp(propertyValue: LynxValue): Iterator[NodeId] = ???

  def greaterThan[T <: Comparable[T]](propertyValue: T): Iterator[Long] = ???
  def smallerThan[T <: Comparable[T]](propertyValue: T): Iterator[Long] = ???

  def contains(propertyValue: LynxString): Iterator[Long] = ???
  def startWith(propertyValue: LynxString): Iterator[Long] = ???
  def endWith(propertyValue: LynxString): Iterator[Long] = ???

}
