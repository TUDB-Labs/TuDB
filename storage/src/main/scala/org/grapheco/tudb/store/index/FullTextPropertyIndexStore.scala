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
