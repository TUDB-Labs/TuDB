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

package org.grapheco.tudb.store.relationship

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 9:28 下午 2022/2/2
  * @Modified By:
  */
trait RelationStoreSPI {
  def allRelationTypes(): Array[String];

  def allRelationTypeIds(): Array[Int];

  def relationCount: Long

  def getRelationTypeName(relationTypeId: Int): Option[String];

  def getRelationTypeId(relationTypeName: String): Option[Int];

  def addRelationType(relationTypeName: String): Int;

  def allPropertyKeys(): Array[String];

  def allPropertyKeyIds(): Array[Int];

  def getPropertyKeyName(keyId: Int): Option[String];

  def getPropertyKeyId(keyName: String): Option[Int];

  def addPropertyKey(keyName: String): Int;

  def getRelationById(relId: Long): Option[StoredRelationshipWithProperty];

  def getRelationIdsByRelationType(relationTypeId: Int): Iterator[Long];

  def relationSetProperty(relationId: Long, propertyKeyId: Int, propertyValue: Any): Unit;

  def relationRemoveProperty(relationId: Long, propertyKeyId: Int): Any;

  def deleteRelation(relationId: Long): Unit;

  def findToNodeIds(fromNodeId: Long): Iterator[Long];

  def findToNodeIds(fromNodeId: Long, relationType: Int): Iterator[Long];

  def findFromNodeIds(toNodeId: Long): Iterator[Long];

  def findFromNodeIds(toNodeId: Long, relationType: Int): Iterator[Long];

  def newRelationId(): Long;

  def addRelation(relation: StoredRelationship): Unit

  def addRelation(relation: StoredRelationshipWithProperty): Unit

  def addRelationship(
      relationshipId: Long,
      fromId: Long,
      toId: Long,
      typeId: Int,
      props: Map[Int, Any]
    )

  def allRelations(): Iterator[StoredRelationship]
  def allRelationsWithProperty(): Iterator[StoredRelationshipWithProperty]

  def findOutRelations(fromNodeId: Long): Iterator[StoredRelationship] =
    findOutRelations(fromNodeId, None)

  def findOutRelations(fromNodeId: Long, edgeType: Option[Int] = None): Iterator[StoredRelationship]

  def findInRelations(toNodeId: Long): Iterator[StoredRelationship] =
    findInRelations(toNodeId, None)

  def findInRelations(toNodeId: Long, edgeType: Option[Int] = None): Iterator[StoredRelationship]

  def findInRelationsBetween(
      toNodeId: Long,
      fromNodeId: Long,
      edgeType: Option[Int] = None
    ): Iterator[StoredRelationship]

  def findOutRelationsBetween(
      fromNodeId: Long,
      toNodeId: Long,
      edgeType: Option[Int] = None
    ): Iterator[StoredRelationship]

  def close(): Unit
}
