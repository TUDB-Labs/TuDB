package org.grapheco.tudb.commons

import org.grapheco.lynx.RelationshipFilter
import org.grapheco.tudb.facade.GraphFacade
import org.grapheco.tudb.graph.{GraphHop, GraphPath}

import scala.collection.mutable.ArrayBuffer

/** @author:John117
  * @createDate:2022/6/27
  * @description:
  */
class BothHopUtils(facade: GraphFacade) {

  // INCOMING transfer to OUTGOING, then each next both-hop we only need to use the last path's endNode to expand
  def getNextBothHop(
      start: GraphHop,
      relationshipFilter: RelationshipFilter
    ): GraphHop = {
    val outGoingPathUtils = new OutGoingPathUtils(facade)
    val inComingPathUtils = new InComingPathUtils(facade)

    val nextHop: ArrayBuffer[GraphPath] = ArrayBuffer.empty

    start.paths.foreach(thisPath => {
      val startNode = thisPath.pathTriples.last.endNode
      // we flag the start node, and next path cannot go back
      val cannotGoBack = thisPath.pathTriples.last.startNode
      val nextTriple = {
        outGoingPathUtils
          .getSingleNodeOutGoingPaths(startNode, relationshipFilter)
          .pathTriples
          .filter(p => p.endNode != cannotGoBack) ++
          inComingPathUtils
            .getSingleNodeInComingPaths(startNode, relationshipFilter)
            .pathTriples
            .filter(p => p.startNode != cannotGoBack)
            .map(p => p.revert) // revert, then we only need to use last node to expand.
      }
      nextTriple.foreach(next => {
        nextHop.append(GraphPath(thisPath.pathTriples ++ Seq(next)))
      })
    })

    GraphHop(nextHop.distinct)
  }
}
