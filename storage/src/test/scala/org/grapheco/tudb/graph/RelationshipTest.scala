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

package org.grapheco.tudb.graph

import collection.mutable.Map

import org.junit._

class RelationshipTest {

  @Test
  def loadDumpsTest() = {
    val relationshipId = 23
    val from = 0
    val to = 1
    val typeId = 3
    val properties = Map((1, 2), (2, "3"))
    val relationship = new Relationship(relationshipId, from, to, typeId, properties)
    relationship.addProperty(3, "4")
    val bytes = relationship.dumps()
    val parsedRelationship = Relationship.loads(bytes)
    Assert.assertEquals(relationship.property(3), parsedRelationship.property(3))
  }

  @Test
  def propertyTest() = {
    val relationshipId = 23
    val from = 0
    val to = 1
    val typeId = 3
    val properties = Map((1, 2), (2, "3"))
    val relationship = new Relationship(relationshipId, from, to, typeId, properties)
    relationship.addProperty(3, "4")
    Assert.assertEquals("4", relationship.property(3).get)
  }
}
