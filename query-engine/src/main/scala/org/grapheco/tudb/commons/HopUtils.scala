package org.grapheco.tudb.commons

import org.grapheco.lynx.physical.filters.RelationshipFilter
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
  def getNextOutGoingHop(start: GraphHop, relationshipFilter: RelationshipFilter): GraphHop = {

    // Save single hop's paths.
    val nextHop: ArrayBuffer[GraphPath] = ArrayBuffer.empty

    start.paths.foreach(thisPath => {
      // From path's last pathTriple's endNode to expand.
      val left = thisPath.pathTriples.last.endNode
      val paths = pathUtils.getSingleNodeOutgoingPaths(left, relationshipFilter)
      /*
                EndNode may expand many paths, then add each new-path to thisPath's last respectively.

                we should filter the circle situation:
                  1 -friend-> 2 -friend-> 3 -friend-> 4 -friend-> 1 then from 1, circle will happen.
                  pathTriple is like below
                                                                          circle happened
                                                                                ||
                                                                                \/
                  (1,friend,2), (2,friend,3), (3,friend,4), (4,friend,1), (1, friend, 2)
       */
      paths.foreach(graphPath => {
        graphPath.pathTriples.foreach(nextOut => {
          if (!thisPath.pathTriples.contains(nextOut))
            nextHop.append(GraphPath(thisPath.pathTriples ++ Seq(nextOut)))
        })
      })
    })
    GraphHop(nextHop)
  }

  def getNextInComingHop(start: GraphHop, relationshipFilter: RelationshipFilter): GraphHop = {
    // save single hop's paths
    val nextHop: ArrayBuffer[GraphPath] = ArrayBuffer.empty

    start.paths.foreach(thisPath => {
      val right = thisPath.pathTriples.last.endNode
      val paths = pathUtils.getSingleNodeIncomingPaths(right, relationshipFilter)
      // same as outgoing
      paths.foreach(graphPath => {
        graphPath.pathTriples.foreach(nextIn => {
          if (!thisPath.pathTriples.contains(nextIn))
            nextHop.append(GraphPath(thisPath.pathTriples ++ Seq(nextIn)))
        })
      })
    })
    GraphHop(nextHop)
  }

  // INCOMING transfer to OUTGOING, then each next both-hop we only need to use the last path's endNode to expand
  def getNextBothHop(start: GraphHop, relationshipFilter: RelationshipFilter): GraphHop = {
    val nextHop: ArrayBuffer[GraphPath] = ArrayBuffer.empty

    start.paths.foreach(thisPath => {
      val startNode = thisPath.pathTriples.last.endNode
      // we flag the start node, and next path cannot go back
      val cannotGoBack = thisPath.pathTriples.last.startNode
      val nextTriple = {
        val out = pathUtils
          .getSingleNodeOutgoingPaths(startNode, relationshipFilter)
          .filter(p => p.endNode != cannotGoBack)

        val in = pathUtils
          .getSingleNodeIncomingPaths(startNode, relationshipFilter)
          .filter(p => p.endNode() != cannotGoBack)
        out ++ in
      }
      nextTriple.foreach(next => {
        if (!thisPath.pathTriples.contains(next.pathTriples.head))
          nextHop.append(GraphPath(thisPath.pathTriples ++ Seq(next.pathTriples.head)))
      })
    })

    GraphHop(nextHop)
  }
}
