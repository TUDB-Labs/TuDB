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

package org.grapheco.lynx.graph

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey, LynxRelationshipType}

/** Recording the number of nodes and relationships under various conditions for optimization.
  */
trait Statistics {

  /** Count the number of nodes.
    * @return The number of nodes
    */
  def numNode: Long

  /** Count the number of nodes containing specified label.
    * @param labelName The label to contained
    * @return The number of nodes meeting conditions
    */
  def numNodeByLabel(labelName: LynxNodeLabel): Long

  /** Count the number of nodes containing specified label and property.
    * @param labelName The label to contained
    * @param propertyName The property to contained
    * @param value The value of this property
    * @return The number of nodes meeting conditions
    */
  def numNodeByProperty(
      labelName: LynxNodeLabel,
      propertyName: LynxPropertyKey,
      value: LynxValue
    ): Long

  /** Count the number of relationships.
    * @return The number of relationships
    */
  def numRelationship: Long

  /** Count the number of relationships have specified type.
    * @param typeName The type
    * @return The number of relationships meeting conditions.
    */
  def numRelationshipByType(typeName: LynxRelationshipType): Long
}
