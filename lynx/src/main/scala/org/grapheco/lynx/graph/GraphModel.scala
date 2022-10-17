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

import org.grapheco.lynx.physical.filters.{NodeFilter, RelationshipFilter}
import org.grapheco.lynx.{ConstrainViolatedException, NoIndexManagerException}
import org.grapheco.lynx.physical.{NodeInput, RelationshipInput}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxId, LynxNode, LynxNodeLabel, LynxPropertyKey, LynxRelationship, LynxRelationshipType}
import org.opencypher.v9_0.expressions.SemanticDirection
import org.opencypher.v9_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}

import scala.collection.mutable.ArrayBuffer

/**
  *@description:
  */
trait GraphModel {

  /** A Statistics object needs to be returned.
    * In the default implementation, those number is obtained through traversal and filtering.
    * You can override the default implementation.
    * @return The Statistics object
    */
  def statistics: Statistics = new Statistics {
    override def numNode: Long = nodes().length

    override def numNodeByLabel(labelName: LynxNodeLabel): Long =
      nodes(
        NodeFilter(Seq(labelName), Map.empty)
      ).length

    override def numNodeByProperty(
        labelName: LynxNodeLabel,
        propertyName: LynxPropertyKey,
        value: LynxValue
      ): Long =
      nodes(NodeFilter(Seq(labelName), Map(propertyName -> value))).length

    override def numRelationship: Long = relationships().length

    override def numRelationshipByType(typeName: LynxRelationshipType): Long =
      relationships(RelationshipFilter(Seq(typeName), Map.empty)).length
  }

  /** An IndexManager object needs to be returned.
    * In the default implementation, the returned indexes is empty,
    * and the addition and deletion of any index will throw an exception.
    * You need override the default implementation.
    * @return The IndexManager object
    */
  def indexManager: IndexManager = new IndexManager {
    override def createIndex(index: Index): Unit = throw NoIndexManagerException(
      s"There is no index manager to handle index creation"
    )

    override def dropIndex(index: Index): Unit = throw NoIndexManagerException(
      s"There is no index manager to handle index dropping"
    )

    override def indexes: Array[Index] = Array.empty
  }

  /** An WriteTask object needs to be returned.
    * There is no default implementation, you must override it.
    * @return The WriteTask object
    */
  def write: WriteTask

  /** All nodes.
    * @return An Iterator of all nodes.
    */
  def nodes(): Iterator[LynxNode]

  /** All nodes with a filter.
    * @param nodeFilter The filter
    * @return An Iterator of all nodes after filter.
    */
  def nodes(nodeFilter: NodeFilter): Iterator[LynxNode] = nodes().filter(nodeFilter.matches)

  /** Return all relationships as PathTriple.
    * @return An Iterator of PathTriple
    */
  def relationships(): Iterator[PathTriple]

  /** Return all relationships as PathTriple with a filter.
    * @param relationshipFilter The filter
    * @return An Iterator of PathTriple after filter
    */
  def relationships(relationshipFilter: RelationshipFilter): Iterator[PathTriple] =
    relationships().filter(f => relationshipFilter.matches(f.storedRelation))

  def createElements[T](
      nodesInput: Seq[(String, NodeInput)],
      relationshipsInput: Seq[(String, RelationshipInput)],
      onCreated: (Seq[(String, LynxNode)], Seq[(String, LynxRelationship)]) => T
    ): T =
    this.write.createElements(nodesInput, relationshipsInput, onCreated)

  def deleteRelations(ids: Iterator[LynxId]): Unit = this.write.deleteRelations(ids)

  def deleteNodes(ids: Seq[LynxId]): Unit = this.write.deleteNodes(ids)

  def setNodesProperties(
      nodeIds: Iterator[LynxId],
      data: Array[(String, Any)],
      cleanExistProperties: Boolean = false
    ): Iterator[Option[LynxNode]] =
    this.write.setNodesProperties(
      nodeIds,
      data.map(kv => (LynxPropertyKey(kv._1), kv._2)),
      cleanExistProperties
    )

  def setNodesLabels(nodeIds: Iterator[LynxId], labels: Array[String]): Iterator[Option[LynxNode]] =
    this.write.setNodesLabels(nodeIds, labels.map(LynxNodeLabel))

  def setRelationshipsProperties(
      relationshipIds: Iterator[LynxId],
      data: Array[(String, Any)]
    ): Iterator[Option[LynxRelationship]] =
    this.write
      .setRelationshipsProperties(relationshipIds, data.map(kv => (LynxPropertyKey(kv._1), kv._2)))

  def setRelationshipsType(
      relationshipIds: Iterator[LynxId],
      theType: String
    ): Iterator[Option[LynxRelationship]] =
    this.write.setRelationshipsType(relationshipIds, LynxRelationshipType(theType))

  def removeNodesProperties(
      nodeIds: Iterator[LynxId],
      data: Array[String]
    ): Iterator[Option[LynxNode]] =
    this.write.removeNodesProperties(nodeIds, data.map(LynxPropertyKey))

  def removeNodesLabels(
      nodeIds: Iterator[LynxId],
      labels: Array[String]
    ): Iterator[Option[LynxNode]] =
    this.write.removeNodesLabels(nodeIds, labels.map(LynxNodeLabel))

  def removeRelationshipsProperties(
      relationshipIds: Iterator[LynxId],
      data: Array[String]
    ): Iterator[Option[LynxRelationship]] =
    this.write.removeRelationshipsProperties(relationshipIds, data.map(LynxPropertyKey))

  def removeRelationshipType(
      relationshipIds: Iterator[LynxId],
      theType: String
    ): Iterator[Option[LynxRelationship]] =
    this.write.removeRelationshipsType(relationshipIds, LynxRelationshipType(theType))

  /** before delete nodes we should check is the nodes have relationships.
    * if nodes have relationships but not force to delete, we should throw exception,
    * otherwise we should delete relationships first then delete nodes.
    *
    * @param nodesIDs The ids of nodes to deleted
    * @param forced When some nodes have relationships,
    *               if it is true, delete any related relationships,
    *               otherwise throw an exception
    */
  def deleteNodesSafely(nodesIDs: Iterator[LynxId], forced: Boolean): Unit = {
    val ids = nodesIDs.toSet
    val affectedRelationships = relationships()
      .map(_.storedRelation)
      .filter(rel => ids.contains(rel.startNodeId) || ids.contains(rel.endNodeId))
    if (affectedRelationships.nonEmpty) {
      if (forced)
        this.write.deleteRelations(affectedRelationships.map(_.id))
      else
        throw ConstrainViolatedException(
          s"deleting nodes with relationships, if force to delete, please use DETACH DELETE."
        )
    }
    this.write.deleteNodes(ids.toSeq)
  }

  def commit(): Boolean = this.write.commit

  /** Get the paths that meets the conditions
    * @param startNodeFilter Filter condition of starting node
    * @param relationshipFilter Filter conditions for relationships
    * @param endNodeFilter Filter condition of ending node
    * @param direction Direction of relationships, INCOMING, OUTGOING or BOTH
    * @param upperLimit Upper limit of relationship length
    * @param lowerLimit Lower limit of relationship length
    * @return The paths
    */
  def paths(
      startNodeFilter: NodeFilter,
      relationshipFilter: RelationshipFilter,
      endNodeFilter: NodeFilter,
      direction: SemanticDirection,
      upperLimit: Option[Int],
      lowerLimit: Option[Int]
    ): Iterator[Seq[PathTriple]] = {

    val lowerHop = lowerLimit.getOrElse(1)
    val upperHop = upperLimit.getOrElse(1)
    if (lowerHop == 1 && upperHop == 1) {
      (direction match {
        case BOTH     => relationships().flatMap(item => Seq(item, item.revert))
        case INCOMING => relationships().map(_.revert)
        case OUTGOING => relationships()
      }).filter {
          case PathTriple(startNode, rel, endNode, _) =>
            relationshipFilter.matches(rel) && startNodeFilter.matches(startNode) && endNodeFilter
              .matches(endNode)
        }
        .map(Seq(_))
    } else {
      // there has some risk of OOM, should override it.
      GraphModelHelper(this).multipleHopSearch(
        startNodeFilter,
        relationshipFilter,
        endNodeFilter,
        lowerHop,
        upperHop,
        direction
      )
    }
  }

  /** Take a node as the starting or ending node and expand in a certain direction.
    * @param nodeId The id of this node
    * @param direction The direction of expansion, INCOMING, OUTGOING or BOTH
    * @return Triples after expansion
    */
  def expand(node: LynxNode, direction: SemanticDirection): Iterator[PathTriple] = {
    direction match {
      case BOTH =>
        relationships()
          .flatMap(item => Seq(item, item.revert))
          .filter(r => r.startNode.id == node.id || r.endNode.id == node.id)
      case INCOMING => relationships().map(_.revert).filter(_.startNode.id == node.id)
      case OUTGOING => relationships().filter(_.startNode.id == node.id)
    }
  }

  /** Take a node as the starting or ending node and expand in a certain direction with some filter.
    * @param nodeId The id of this node
    * @param relationshipFilter conditions for relationships
    * @param endNodeFilter Filter condition of ending node
    * @param direction The direction of expansion, INCOMING, OUTGOING or BOTH
    * @return Triples after expansion and filter
    */
  def expand(
      node: LynxNode,
      relationshipFilter: RelationshipFilter,
      endNodeFilter: NodeFilter,
      direction: SemanticDirection,
      lowerLimit: Int,
      upperLimit: Int
    ): Iterator[Seq[PathTriple]] = {
    expand(node, direction)
      .filter { pathTriple =>
        direction match {
          case SemanticDirection.OUTGOING =>
            relationshipFilter.matches(pathTriple.storedRelation) && endNodeFilter.matches(
              pathTriple.endNode
            )
          case SemanticDirection.INCOMING =>
            relationshipFilter.matches(pathTriple.storedRelation) && endNodeFilter.matches(
              pathTriple.startNode
            )
          case SemanticDirection.BOTH =>
            relationshipFilter.matches(pathTriple.storedRelation) &&
              (
                endNodeFilter.matches(pathTriple.startNode) || endNodeFilter.matches(
                  pathTriple.endNode
                )
              )
        }
      }
      .filter(p => p.endNode != node)
      .map(Seq(_))
  }

  /** GraphHelper
    */
  val _helper: GraphModelHelper = GraphModelHelper(this)
}
