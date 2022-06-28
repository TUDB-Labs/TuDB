package org.grapheco.tudb.commons

import org.grapheco.lynx.{PathTriple, RelationshipFilter}
import org.grapheco.lynx.types.structural.LynxNode
import org.grapheco.tudb.facade.GraphFacade
import org.grapheco.tudb.graph.GraphPath
import org.grapheco.tudb.store.relationship.StoredRelationship

/** @author:John117
  * @createDate:2022/6/27
  * @description:
  */
class InComingPathUtils(facade: GraphFacade) {

  /** @param endNode the node to search it's incoming relationships
    * @param relationshipFilter filter the searched relationships
    * @return filtered pathTriple
    */
  def getSingleNodeInComingPaths(
      endNode: LynxNode,
      relationshipFilter: RelationshipFilter
    ): GraphPath = {
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
      rels.map(r => PathTriple(facade.nodeAt(r.from).get, facade.relationshipAt(r.id).get, endNode))
    })

    // filter relationships
    GraphPath(pathTriples.filter(p => relationshipFilter.matches(p.storedRelation)))
  }
}
