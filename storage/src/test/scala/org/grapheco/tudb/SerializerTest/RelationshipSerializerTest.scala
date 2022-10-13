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

package org.grapheco.tudb.SerializerTest

import org.grapheco.tudb.serializer.RelationshipSerializer
import org.junit.{Assert, Test}

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 9:09 下午 2022/1/26
  * @Modified By:
  */
class RelationshipSerializerTest {
  val relationshipID1 = 123456L
  val fromID1 = 100L
  val toID1 = 200L
  val typeID1 = 1
  val props1: Map[Int, Any] = Map(1 -> 1, 2 -> 2L, 3 -> 3.0, 4 -> "4.00")

  val relationshipID2 = 23456L
  val fromID2 = 200L
  val toID2 = 300L
  val typeID2 = 2
  val props2: Map[Int, Any] = Map(5 -> 5, 6 -> 6.0f, 7 -> "7.0", 8 -> 8.0)

  val serializedRelationship1 =
    RelationshipSerializer.encodeRelationship(
      relationshipID1,
      fromID1,
      toID1,
      typeID1,
      props1
    )

  val serializedRelationship2 =
    RelationshipSerializer.encodeRelationship(
      relationshipID2,
      fromID2,
      toID2,
      typeID2,
      props2
    )

  @Test
  def testSingleRelationship(): Unit = {
    val relationship = RelationshipSerializer.decodeRelationshipWithProperties(
      serializedRelationship1
    )
    Assert.assertEquals(relationshipID1, relationship.id)
    Assert.assertEquals(fromID1, relationship.from)
    Assert.assertEquals(toID1, relationship.to)
    Assert.assertEquals(typeID1, relationship.typeId)
    Assert.assertTrue(relationship.properties.sameElements(props1))
  }

}
