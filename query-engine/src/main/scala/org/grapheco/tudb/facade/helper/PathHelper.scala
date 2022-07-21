package org.grapheco.tudb.facade.helper

import org.grapheco.lynx.{NodeFilter, PathTriple, RelationshipFilter}
import org.grapheco.lynx.types.structural.LynxNode
import org.grapheco.tudb.commons.{HopUtils, PathUtils}
import org.grapheco.tudb.facade.GraphFacade
import org.grapheco.tudb.graph.{GraphHop, GraphPath}
import org.opencypher.v9_0.expressions.SemanticDirection

import scala.collection.mutable.ArrayBuffer

/**
  *@author:John117
  *@createDate:2022/7/21
  *@description:
  */
class PathHelper(graphFacade: GraphFacade) {
  // logic like pathWithLength

  def getPathWithoutLength(
      startNodeFilter: NodeFilter,
      relationshipFilter: RelationshipFilter,
      endNodeFilter: NodeFilter,
      direction: SemanticDirection
    ): Iterator[PathTriple] = {
    direction match {
      case SemanticDirection.OUTGOING => {
        val pathUtils = new PathUtils(graphFacade)

        val startNodes = graphFacade.nodes(startNodeFilter)
        startNodes
          .flatMap(startNode => pathUtils.getSingleNodeOutgoingPaths(startNode, relationshipFilter))
          .filter(p => endNodeFilter.matches(p.endNode()))
          .flatMap(f => f.pathTriples)
      }
      case SemanticDirection.INCOMING => {
        val inComingPathUtils = new PathUtils(graphFacade)

        graphFacade
          .nodes(startNodeFilter)
          .flatMap(startNode =>
            inComingPathUtils.getSingleNodeIncomingPaths(startNode, relationshipFilter)
          )
          .filter(p => endNodeFilter.matches(p.endNode()))
          .flatMap(f => f.pathTriples)
      }
      case SemanticDirection.BOTH => {
        val pathUtils = new PathUtils(graphFacade)

        val startNodes = graphFacade.nodes(startNodeFilter).duplicate
        (startNodes._1
          .flatMap(startNode => pathUtils.getSingleNodeOutgoingPaths(startNode, relationshipFilter))
          .filter(p => endNodeFilter.matches(p.endNode())) ++
          startNodes._2
            .flatMap(endNode => pathUtils.getSingleNodeIncomingPaths(endNode, relationshipFilter))
            .filter(p => endNodeFilter.matches(p.endNode()))).flatMap(f => f.pathTriples)

      }
    }
  }

  /**  minHops and maxHops are optional and default to 1 and infinity respectively.
    *
    *  If the path length between two nodes is zero, they are by definition the same node.
    *  Note that when matching zero length paths the result may contain a match
    *  even when matching on a relationship type not in use.
    *
    * [:TYPE*minHops..maxHops] = query fixed range relationships
    * [:TYPE*minHops..]  = query relationships from minHops to INF
    * [:TYPE*..maxHops]  = query relationships from 1 to maxHops
    * [:TYPE*Hops] = query fixed length relationships
    *
    *  p = PathTriple(START_NODE, RELATIONSHIP, END_NODE)
    *      eg: match (n: Person)-[r:TYPE*1..3]->(m: Person)
    *      hop1 ==>   Seq( Seq(p1), Seq(p2), Seq(p3), Seq(p4), Seq(p5) ) // five single relationships
    *      hop2 ==>   Seq( Seq(p1, p2), Seq(p3, p4) ) // two hop-2 relationships
    *      hop3 ==>   Seq( Seq(p1, p2, p5) ) // one hop-3 relationships
    *
    *      Total: hop1 ++ hop2 ++ hop3 =
    *         Seq(
    *              Seq( Seq(p1), Seq(p2), Seq(p3), Seq(p4), Seq(p5) ),
    *              Seq( Seq(p1, p2), Seq(p3, p4) ),
    *              Seq( Seq(p1, p2, p5) )
    *            )
    *
    */
  def getPathsWithLength(
      startNodeFilter: NodeFilter,
      relationshipFilter: RelationshipFilter,
      endNodeFilter: NodeFilter,
      direction: SemanticDirection,
      lowerLimit: Int,
      upperLimit: Int
    ): Iterator[Seq[PathTriple]] = {
    direction match {
      // Outgoing relationships.
      case SemanticDirection.OUTGOING => {
        // Get hops from 1 to lowerLimit, like (a)-[r:TYPE*3..5]->(b), we should init hop to hop-3.
        val hopsToLowerLimit =
          initInOutStartHop(startNodeFilter, relationshipFilter, direction, lowerLimit)

        val hops = getOutGoingPathsWithLength(
          hopsToLowerLimit,
          relationshipFilter,
          endNodeFilter,
          lowerLimit,
          upperLimit
        )
        val r1 = hops.map(hop => hop.paths.map(path => path.pathTriples))
        r1.foldLeft(Seq.empty[Seq[PathTriple]])((a, b) => a ++ b).toIterator
      }
      case SemanticDirection.INCOMING => {
        val hopsToLowerLimit =
          initInOutStartHop(startNodeFilter, relationshipFilter, direction, lowerLimit)
        val hops = getInComingLengthPaths(
          hopsToLowerLimit,
          relationshipFilter,
          endNodeFilter,
          lowerLimit,
          upperLimit
        )
        val r1 = hops.map(hop => hop.paths.map(path => path.pathTriples))
        r1.foldLeft(Seq.empty[Seq[PathTriple]])((a, b) => a ++ b).toIterator
      }
      case SemanticDirection.BOTH => {
        // different from incoming or outgoing, expand from each node, we all need to find in and out relationships
        val hopsToLowerLimit = initBothStartHop(startNodeFilter, relationshipFilter, lowerLimit)
        val hops = getBothLengthPaths(
          hopsToLowerLimit,
          relationshipFilter,
          startNodeFilter,
          endNodeFilter,
          lowerLimit,
          upperLimit
        )
        val r1 = hops.map(hop => hop.paths.map(path => path.pathTriples))
        r1.foldLeft(Seq.empty[Seq[PathTriple]])((a, b) => a ++ b).toIterator
      }
    }
  }

  private def getOutGoingPathsWithLength(
      hopsToLowerLimit: Seq[GraphHop],
      relationshipFilter: RelationshipFilter,
      endNodeFilter: NodeFilter,
      lowerLimit: Int,
      upperLimit: Int
    ): Seq[GraphHop] = {

    val hopUtils = new HopUtils(new PathUtils(graphFacade))

    val collectedResult: ArrayBuffer[GraphHop] = ArrayBuffer.empty
    val filteredResult: ArrayBuffer[GraphHop] = ArrayBuffer.empty

    // If (a)-[r:TYPE*3..5]->(b), then hopsToLowerLimit have the hops from 1 to 3, we only need to get hop3's paths
    var nextHop = {
      if (lowerLimit == 0 || lowerLimit == 1) hopsToLowerLimit.head
      else hopsToLowerLimit(lowerLimit - 1)
    }
    collectedResult.append(nextHop)
    var count = {
      if (lowerLimit == 0) 0
      else lowerLimit + 1
    }
    var flag = {
      if (upperLimit != 0) true
      else false
    }

    // Loop to reach the upperLimit, if nextHop is empty, loop stop.
    while (count <= upperLimit && flag) {
      count += 1
      nextHop = hopUtils.getNextOutGoingHop(nextHop, relationshipFilter)
      if (nextHop.paths.nonEmpty) {
        collectedResult.append(nextHop)
      } else flag = false
    }

    // Filter loop result
    collectedResult.foreach(hops => {
      val filteredPaths =
        hops.paths.filter(path => endNodeFilter.matches(path.pathTriples.last.endNode))
      filteredResult.append(GraphHop(filteredPaths))
    })

    filteredResult
  }

  private def initInOutStartHop(
      startNodeFilter: NodeFilter,
      relationshipFilter: RelationshipFilter,
      direction: SemanticDirection,
      lowerLimit: Int
    ): Seq[GraphHop] = {
    val beginNodes = graphFacade.nodes(startNodeFilter)
    lowerLimit match {
      // 0 means relationship to itself, like (a)-->(a)
      case 0 => {
        val res = beginNodes.map(node => GraphPath(Seq(PathTriple(node, null, node)))).toSeq
        Seq(GraphHop(res))
      }
      case _ => {
        direction match {
          case SemanticDirection.OUTGOING => {
            getOutGoingHopFromOne2Limit(beginNodes, relationshipFilter, lowerLimit)
          }
          case SemanticDirection.INCOMING => {
            getInComingHopFromOne2Limit(beginNodes, relationshipFilter, lowerLimit)
          }
        }
      }
    }
  }

  private def getOutGoingHopFromOne2Limit(
      beginNodes: Iterator[LynxNode],
      relationshipFilter: RelationshipFilter,
      lowerLimit: Int
    ): ArrayBuffer[GraphHop] = {
    val pathUtils = new PathUtils(graphFacade)
    val hopUtils = new HopUtils(pathUtils)

    val collected: ArrayBuffer[GraphHop] = ArrayBuffer.empty

    val firstHop = GraphHop(
      beginNodes
        .flatMap(node => pathUtils.getSingleNodeOutgoingPaths(node, relationshipFilter))
        .toSeq
        .filter(p => p.pathTriples.nonEmpty)
    )

    collected.append(firstHop)
    var nextHop: GraphHop = null

    // Iterator to reach lowerLimit.
    var count = 1
    while (count < lowerLimit) {
      count += 1
      nextHop = hopUtils.getNextOutGoingHop(firstHop, relationshipFilter)
      collected.append(nextHop)
    }
    collected
  }

  // Same logic as getOutGoingLengthPath.
  private def getInComingLengthPaths(
      hopsToLowerLimit: Seq[GraphHop],
      relationshipFilter: RelationshipFilter,
      stopNodeFilter: NodeFilter,
      lowerLimit: Int,
      upperLimit: Int
    ): Seq[GraphHop] = {
    val hopUtils = new HopUtils(new PathUtils(graphFacade))
    val collectedResult: ArrayBuffer[GraphHop] = ArrayBuffer.empty
    val filteredResult: ArrayBuffer[GraphHop] = ArrayBuffer.empty

    var nextHop = {
      if (lowerLimit == 0 || lowerLimit == 1) hopsToLowerLimit.head
      else hopsToLowerLimit(lowerLimit - 1)
    }
    collectedResult.append(nextHop)
    var count = {
      if (lowerLimit == 0) 0
      else lowerLimit + 1
    }
    var flag = {
      if (upperLimit != 0) true
      else false
    }
    while (count <= upperLimit && flag) {
      count += 1
      nextHop = hopUtils.getNextInComingHop(nextHop, relationshipFilter)
      if (nextHop.paths.nonEmpty) {
        collectedResult.append(nextHop)
      } else flag = false
    }
    collectedResult.foreach(hops => {
      val filteredPaths =
        hops.paths.filter(path => stopNodeFilter.matches(path.pathTriples.last.endNode))
      filteredResult.append(GraphHop(filteredPaths))
    })

    filteredResult
  }

  // Same logic as getOutGoingHopFromOne2Limit
  private def getInComingHopFromOne2Limit(
      beginNodes: Iterator[LynxNode],
      relationshipFilter: RelationshipFilter,
      lowerLimit: Int
    ): ArrayBuffer[GraphHop] = {
    val pathUtils = new PathUtils(graphFacade)
    val hopUtils = new HopUtils(pathUtils)

    val collected: ArrayBuffer[GraphHop] = ArrayBuffer.empty
    val firstHop = GraphHop(
      beginNodes
        .flatMap(node => pathUtils.getSingleNodeIncomingPaths(node, relationshipFilter))
        .toSeq
        .filter(p => p.pathTriples.nonEmpty)
    )

    collected.append(firstHop)
    var nextHop: GraphHop = null
    var count = 1
    while (count < lowerLimit) {
      count += 1
      nextHop = hopUtils.getNextInComingHop(firstHop, relationshipFilter)
      collected.append(nextHop)
    }
    collected
  }

  // need both for every hop
  private def initBothStartHop(
      startNodeFilter: NodeFilter,
      relationshipFilter: RelationshipFilter,
      lowerLimit: Int
    ): Seq[GraphHop] = {
    val beginNodes = graphFacade.nodes(startNodeFilter)
    lowerLimit match {
      case 0 => {
        val res = beginNodes.map(node => GraphPath(Seq(PathTriple(node, null, node)))).toSeq
        Seq(GraphHop(res))
      }
      case _ => {
        getBothHopFromOne2Limit(beginNodes, relationshipFilter, lowerLimit)
      }
    }
  }

  private def getBothLengthPaths(
      hopsToLowerLimit: Seq[GraphHop],
      relationshipFilter: RelationshipFilter,
      startNodeFilter: NodeFilter,
      endNodeFilter: NodeFilter,
      lowerLimit: Int,
      upperLimit: Int
    ): Seq[GraphHop] = {
    val hopUtils = new HopUtils(new PathUtils(graphFacade))

    val collectedResult: ArrayBuffer[GraphHop] = ArrayBuffer.empty
    val filteredResult: ArrayBuffer[GraphHop] = ArrayBuffer.empty

    // same logic as outgoing
    var nextHop = {
      if (lowerLimit == 0 || lowerLimit == 1) hopsToLowerLimit.head
      else hopsToLowerLimit(lowerLimit - 1)
    }
    collectedResult.append(nextHop)
    var count = {
      if (lowerLimit == 0) 0
      else lowerLimit + 1
    }
    var flag = {
      if (upperLimit != 0) true
      else false
    }

    while (count <= upperLimit && flag) {
      count += 1
      nextHop = hopUtils.getNextBothHop(nextHop, relationshipFilter)
      if (nextHop.paths.nonEmpty) {
        collectedResult.append(nextHop)
      } else flag = false
    }

    /*
     Because we search both-path(no direction), so there are two situation to filter:
       1. start node match startNodeFilter and end node match endNodeFilter
       or
       2.start node match endNodeFilter and end node match startNodeFilter
     */
    collectedResult.distinct.foreach(hops => {
      val res = hops.paths.filter(thisPath =>
        startNodeFilter.matches(thisPath.pathTriples.head.startNode) && endNodeFilter.matches(
          thisPath.pathTriples.last.endNode
        )
      )
      filteredResult.append(GraphHop(res))
    })
    filteredResult
  }

  private def getBothHopFromOne2Limit(
      beginNodes: Iterator[LynxNode],
      relationshipFilter: RelationshipFilter,
      lowerLimit: Int
    ): ArrayBuffer[GraphHop] = {
    val pathUtils = new PathUtils(graphFacade)
    val hopUtils = new HopUtils(pathUtils)
    val collected: ArrayBuffer[GraphHop] = ArrayBuffer.empty
    val firstHop = GraphHop(
      beginNodes
        .flatMap(node => pathUtils.getSingleNodeBothPaths(node, relationshipFilter))
        .toSeq
        .filter(p => p.pathTriples.nonEmpty)
    )
    collected.append(firstHop)
    var nextHop: GraphHop = null
    var count = 1
    while (count < lowerLimit) {
      count += 1
      nextHop = hopUtils.getNextBothHop(firstHop, relationshipFilter)
      collected.append(nextHop)
    }
    collected
  }
}
