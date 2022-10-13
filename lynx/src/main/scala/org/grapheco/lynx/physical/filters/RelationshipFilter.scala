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

package org.grapheco.lynx.physical.filters

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxPropertyKey, LynxRelationship, LynxRelationshipType}

/**
  *@description:
  */
/** types note: the relationship of type TYPE1 or of type TYPE2.
  * @param types type names
  * @param properties filter property names
  */
case class RelationshipFilter(
    types: Seq[LynxRelationshipType],
    properties: Map[LynxPropertyKey, LynxValue]) {
  def matches(relationship: LynxRelationship): Boolean =
    ((types, relationship.relationType) match {
      case (Seq(), _)          => true
      case (_, None)           => false
      case (_, Some(typeName)) => types.contains(typeName)
    }) && properties.forall {
      case (propertyName, value) =>
        relationship.property(propertyName).exists(value.equals)
    }
}
