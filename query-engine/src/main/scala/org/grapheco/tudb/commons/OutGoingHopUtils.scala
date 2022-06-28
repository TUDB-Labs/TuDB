package org.grapheco.tudb.commons

import org.grapheco.lynx.{PathTriple, RelationshipFilter}
import org.grapheco.tudb.graph.{GraphHop, GraphPath}

import scala.collection.mutable.ArrayBuffer

/** @author:John117
  * @createDate:2022/6/27
  * @description:
  */
class OutGoingHopUtils(pathUtils: OutGoingPathUtils) {

  /** @param start start hop
    * @param relationshipFilter filter
    * @return next hop paths
    */
  def getNextOutGoingHop(
      start: GraphHop,
      relationshipFilter: RelationshipFilter
    ): GraphHop = {

    // save single hop's paths
    val nextHop: ArrayBuffer[GraphPath] = ArrayBuffer.empty

    start.paths.foreach(thisPath => {
      // from path's last pathTriple's endNode to expand
      val left = thisPath.pathTriples.last.endNode
      val paths = pathUtils.getSingleNodeOutGoingPaths(left, relationshipFilter)
      paths.pathTriples.foreach(next => {
        // endNode may expand many paths, then add each new-path to thisPath's last respectively
        nextHop.append(GraphPath(thisPath.pathTriples ++ Seq(next)))
      })
    })
    GraphHop(nextHop.distinct)
  }
}
