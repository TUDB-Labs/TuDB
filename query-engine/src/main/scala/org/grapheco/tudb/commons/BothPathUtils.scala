package org.grapheco.tudb.commons

import org.grapheco.lynx.RelationshipFilter
import org.grapheco.lynx.types.structural.LynxNode
import org.grapheco.tudb.facade.GraphFacade
import org.grapheco.tudb.graph.GraphPath
import org.grapheco.tudb.store.relationship.StoredRelationship

/** @author:John117
  * @createDate:2022/6/27
  * @description:
  */
class BothPathUtils(facade: GraphFacade) {
  val outGoingPathUtils = new OutGoingPathUtils(facade)
  val inComingPathUtils = new InComingPathUtils(facade)

  def getSingleNodeBothPaths(
      startNode: LynxNode,
      relationshipFilter: RelationshipFilter
    ): GraphPath = {
    val out = outGoingPathUtils.getSingleNodeOutGoingPaths(startNode, relationshipFilter)
    val in = inComingPathUtils.getSingleNodeInComingPaths(startNode, relationshipFilter)
    GraphPath((out.pathTriples ++ in.pathTriples).distinct)
  }
}
