package org.grapheco.tudb.store.index

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxString
import org.grapheco.tudb.store.meta.TypeManager.PropertyId

import scala.concurrent.Future

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created at 17:19 2022/4/24
  * @Modified By:
  */
trait PropertyIndexStore {

  def isIndexed(propertyId: PropertyId): Boolean = ???

  def buildIndex(propertyId: PropertyId): Future[Boolean] = ???
  def dropIndex(propertyId: PropertyId): Future[Boolean] = ???

  def getIdsByProp(propertyId: PropertyId, propertyValue: LynxValue): Iterator[Long] = ???

  def greaterThan[T <: Comparable[T]](propertyId: PropertyId, propertyValue: T): Iterator[Long] =
    ???
  def smallerThan[T <: Comparable[T]](propertyId: PropertyId, propertyValue: T): Iterator[Long] =
    ???

  def contains(propertyId: PropertyId, propertyValue: LynxString): Iterator[Long] = ???
  def startWith(propertyId: PropertyId, propertyValue: LynxString): Iterator[Long] = ???
  def endWith(propertyId: PropertyId, propertyValue: LynxString): Iterator[Long] = ???
}
