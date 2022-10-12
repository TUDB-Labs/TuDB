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
