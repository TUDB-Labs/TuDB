package org.grapheco.tudb.commons

import org.grapheco.lynx.types.structural.LynxNode
import org.grapheco.lynx.{PathTriple, RelationshipFilter}
import org.grapheco.tudb.facade.GraphFacade
import org.grapheco.tudb.graph.GraphPath
import org.grapheco.tudb.store.relationship.StoredRelationship

/** @author:John117
  * @createDate:2022/6/27
  * @description:
  */
class OutGoingPathUtils(facade: GraphFacade) {

  /** @param startNode the node to search it's outgoing relationships
    * @param relationshipFilter filter the searched relationships
    * @return filtered pathTriple
    */

  def getSingleNodeOutGoingPaths(
      startNode: LynxNode,
      relationshipFilter: RelationshipFilter
    ): GraphPath = {
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
    GraphPath(pathTriples.filter(p => relationshipFilter.matches(p.storedRelation)))
  }
}
