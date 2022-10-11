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

package org.grapheco.tudb.commons

import org.grapheco.lynx.graph.PathTriple
import org.grapheco.lynx.physical.filters.RelationshipFilter
import org.grapheco.lynx.types.structural.LynxNode
import org.grapheco.tudb.facade.GraphFacade
import org.grapheco.tudb.graph.GraphPath
import org.grapheco.tudb.store.relationship.StoredRelationship

/** @author:John117
  * @createDate:2022/6/29
  * @description:
  */
class PathUtils(facade: GraphFacade) {

  /** @param startNode the node to search it's outgoing relationships
    * @param relationshipFilter filter the searched relationships
    * @return the node's all outgoing GraphPaths, each GraphPath should be individual
    */
  def getSingleNodeOutgoingPaths(
      startNode: LynxNode,
      relationshipFilter: RelationshipFilter
    ): Seq[GraphPath] = {
    // get relationship type ids
    val relTypeIds = relationshipFilter.types.map(r => facade.relTypeNameToId(r.value))
    // get each type id's outgoing relationships
    val _relationships: Seq[Iterator[StoredRelationship]] = {
      if (relTypeIds.isEmpty) Seq(facade.findOutRelations(startNode.id.toLynxInteger.value))
      else {
        relTypeIds.map(typeId => facade.findOutRelations(startNode.id.toLynxInteger.value, typeId))
      }
    }
    /*
    flatten outgoing relationships and transfer to pathTriples

    facade.findOutRelations() is to search relations index,
    so if we want relationship's properties, we need to get it.
     */
    val pathTriples = _relationships.flatMap(rels => {
      rels.map(r => PathTriple(startNode, facade.relationshipAt(r.id).get, facade.nodeAt(r.to).get))
    })

    // filter relationships
    pathTriples
      .filter(p => relationshipFilter.matches(p.storedRelation))
      .map(pathTriple => GraphPath(Seq(pathTriple)))
  }

  /** @param endNode the node to search it's incoming relationships
    * @param relationshipFilter filter the searched relationships
    * @return filtered pathTriple
    */
  def getSingleNodeIncomingPaths(
      endNode: LynxNode,
      relationshipFilter: RelationshipFilter
    ): Seq[GraphPath] = {
    // get relationship type ids
    val relTypeIds = relationshipFilter.types.map(r => facade.relTypeNameToId(r.value))
    // get each type id's incoming relationships
    val _relationships: Seq[Iterator[StoredRelationship]] = {
      if (relTypeIds.isEmpty) Seq(facade.findInRelations(endNode.id.toLynxInteger.value))
      else {
        relTypeIds.map(typeId => facade.findInRelations(endNode.id.toLynxInteger.value, typeId))
      }
    }
    /*
      flatten incoming relationships and transfer to pathTriples

      facade.findInRelations() is to search relations index,
      so if we want relationship's properties, we need to get it.
     */
    val pathTriples = _relationships.flatMap(rels => {
      rels.map(r =>
        PathTriple(endNode, facade.relationshipAt(r.id).get, facade.nodeAt(r.from).get, true)
      )
    })

    // filter relationships
    pathTriples
      .filter(p => relationshipFilter.matches(p.storedRelation))
      .map(pathTriple => GraphPath(Seq(pathTriple)))
  }

  def getSingleNodeBothPaths(
      startNode: LynxNode,
      relationshipFilter: RelationshipFilter
    ): Seq[GraphPath] = {
    val out = getSingleNodeOutgoingPaths(startNode, relationshipFilter)
    val in = getSingleNodeIncomingPaths(startNode, relationshipFilter)
    (out.flatMap(f => f.pathTriples) ++ in.flatMap(f => f.pathTriples)).map(triple =>
      GraphPath(Seq(triple))
    )

  }
}
