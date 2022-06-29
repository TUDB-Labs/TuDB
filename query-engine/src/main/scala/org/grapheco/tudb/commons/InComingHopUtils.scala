package org.grapheco.tudb.commons

import org.grapheco.lynx.{PathTriple, RelationshipFilter}
import org.grapheco.tudb.graph.{GraphHop, GraphPath}

import scala.collection.mutable.ArrayBuffer

/** @author:John117
  * @createDate:2022/6/27
  * @description:
  */
class InComingHopUtils(pathUtils: InComingPathUtils) {

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
        nextHop.append(GraphPath(Seq(next) ++ thisPath.pathTriples))
      })
    })
    GraphHop(nextHop.distinct)
  }
}
