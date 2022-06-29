package org.grapheco.tudb.commons

import org.grapheco.lynx.RelationshipFilter
import org.grapheco.tudb.facade.GraphFacade
import org.grapheco.tudb.graph.{GraphHop, GraphPath}

import scala.collection.mutable.ArrayBuffer

/** @author:John117
  * @createDate:2022/6/29
  * @description:
  */
class HopUtils(pathUtils: PathUtils) {

  /** @param start start hop
    * @param relationshipFilter filter
    * @return next hop paths
    */
  def getNextOutGoingHop(
      start: GraphHop,
      relationshipFilter: RelationshipFilter
    ): GraphHop = {

    // Save single hop's paths.
    val nextHop: ArrayBuffer[GraphPath] = ArrayBuffer.empty

    start.paths.foreach(thisPath => {
      // From path's last pathTriple's endNode to expand.
      val left = thisPath.pathTriples.last.endNode
      val paths = pathUtils.getSingleNodeOutGoingPaths(left, relationshipFilter)
      paths.pathTriples.foreach(next => {
        /*
          EndNode may expand many paths, then add each new-path to thisPath's last respectively.

          we should filter the circle situation:
            1 -friend-> 2 -friend-> 3 -friend-> 4 -friend-> 1
            , then from 1, circle will happen.
            pathTriple's like below
                                                                    circle happened
                                                                          ||
                                                                          \/
            (1,friend,2), (2,friend,3), (3,friend,4), (4,friend,1), (1, friend, 2)
         */
        if (!thisPath.pathTriples.contains(next))
          nextHop.append(GraphPath(thisPath.pathTriples ++ Seq(next)))
      })
    })
    GraphHop(nextHop)
  }

  def getNextInComingHop(
      start: GraphHop,
      relationshipFilter: RelationshipFilter
    ): GraphHop = {
    // save single hop's paths
    val nextHop: ArrayBuffer[GraphPath] = ArrayBuffer.empty

    start.paths.foreach(thisPath => {
      // from path's first pathTriple's startNode to expand
      val right = thisPath.pathTriples.head.startNode
      val paths = pathUtils.getSingleNodeInComingPaths(right, relationshipFilter)
      paths.pathTriples.foreach(next => {
        // startNode may expand many paths, then add each new-path to thisPath's head respectively
        if (!thisPath.pathTriples.contains(next))
          nextHop.append(GraphPath(Seq(next) ++ thisPath.pathTriples))
      })
    })
    GraphHop(nextHop)
  }

  // INCOMING transfer to OUTGOING, then each next both-hop we only need to use the last path's endNode to expand
  def getNextBothHop(
      start: GraphHop,
      relationshipFilter: RelationshipFilter
    ): GraphHop = {
    val nextHop: ArrayBuffer[GraphPath] = ArrayBuffer.empty

    start.paths.foreach(thisPath => {
      val startNode = thisPath.pathTriples.last.endNode
      // we flag the start node, and next path cannot go back
      val cannotGoBack = thisPath.pathTriples.last.startNode
      val nextTriple = {
        pathUtils
          .getSingleNodeOutGoingPaths(startNode, relationshipFilter)
          .pathTriples
          .filter(p => p.endNode != cannotGoBack) ++
          pathUtils
            .getSingleNodeInComingPaths(startNode, relationshipFilter)
            .pathTriples
            .filter(p => p.startNode != cannotGoBack)
            .map(p => p.revert) // revert, then we only need to use last node to expand.
      }
      nextTriple.foreach(next => {
        if (!thisPath.pathTriples.contains(next))
          nextHop.append(GraphPath(thisPath.pathTriples ++ Seq(next)))
      })
    })

    GraphHop(nextHop)
  }
}
