package org.grapheco.tudb.engine

import org.grapheco.tudb.graph.Relationship

trait RelationshipOperations {
  def allRelationshipTypes(): Array[String];

  def allRelationshipTypeIds(): Array[Int];

  def relationCount: Long

  def getRelationshipTypeName(relationTypeId: Int): Option[String];

  def getRelationshipTypeId(relationTypeName: String): Option[Int];

  def addRelationshipType(relationTypeName: String): Int;

  def allPropertyKeys(): Array[String];

  def allPropertyKeyIds(): Array[Int];

  def getPropertyKeyName(keyId: Int): Option[String];

  def getPropertyKeyId(keyName: String): Option[Int];

  def addPropertyKey(keyName: String): Int;

  def getRelationshipById(relId: Long): Option[Relationship];

  def getRelationshipIdsByRelationshipType(relationTypeId: Int): Iterator[Long];

  def relationshipSetProperty(relationId: Long, propertyKeyId: Int, propertyValue: Any): Unit;

  def relationshipRemoveProperty(relationId: Long, propertyKeyId: Int): Any;

  def deleteRelationship(relationId: Long): Unit;

  def findToNodeIds(fromNodeId: Long): Iterator[Long];

  def findToNodeIds(fromNodeId: Long, relationType: Int): Iterator[Long];

  def findFromNodeIds(toNodeId: Long): Iterator[Long];

  def findFromNodeIds(toNodeId: Long, relationType: Int): Iterator[Long];

  def newRelationshipId(): Long;

  def addRelationship(relation: Relationship): Unit

  def addRelationship(
      relationshipId: Long,
      fromId: Long,
      toId: Long,
      typeId: Int,
      props: Map[Int, Any]
    )

  def allRelationships(): Iterator[Relationship]
  def allRelationshipsWithProperty(): Iterator[Relationship]

  def findOutRelationships(fromNodeId: Long): Iterator[Relationship] =
    findOutRelationships(fromNodeId, None)

  def findOutRelationships(fromNodeId: Long, edgeType: Option[Int] = None): Iterator[Relationship]

  def findInRelationships(toNodeId: Long): Iterator[Relationship] =
    findInRelationships(toNodeId, None)

  def findInRelationships(toNodeId: Long, edgeType: Option[Int] = None): Iterator[Relationship]

  def findInRelationshipsBetween(
      toNodeId: Long,
      fromNodeId: Long,
      edgeType: Option[Int] = None
    ): Iterator[Relationship]

  def findOutRelationshipsBetween(
      fromNodeId: Long,
      toNodeId: Long,
      edgeType: Option[Int] = None
    ): Iterator[Relationship]

  def close(): Unit
}
