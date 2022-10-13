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

package org.grapheco.tudb.facade.helper

import org.grapheco.lynx.graph.PathTriple
import org.grapheco.lynx.physical.filters.{NodeFilter, RelationshipFilter}
import org.grapheco.lynx.types.structural.LynxNode
import org.grapheco.tudb.commons.{HopUtils, PathUtils}
import org.grapheco.tudb.facade.GraphFacade
import org.grapheco.tudb.graph.{GraphHop, GraphPath}
import org.opencypher.v9_0.expressions.SemanticDirection

import scala.collection.mutable.ArrayBuffer

/**
  *@author:John117
  *@createDate:2022/7/20
  *@description: Copy from PathHelper, and only modify the startNodeFilter:NodeFilter to startNode:LynxNode
  */
class ExpandFromNodeHelper(graphFacade: GraphFacade) {

  def getExpandPathWithoutLength(
      startNode: LynxNode,
      relationshipFilter: RelationshipFilter,
      endNodeFilter: NodeFilter,
      direction: SemanticDirection
    ): Iterator[PathTriple] = {
    direction match {
      case SemanticDirection.OUTGOING => {
        val pathUtils = new PathUtils(graphFacade)
        Iterator(startNode)
          .flatMap(startNode => pathUtils.getSingleNodeOutgoingPaths(startNode, relationshipFilter))
          .filter(p => endNodeFilter.matches(p.endNode()))
          .flatMap(f => f.pathTriples)
      }
      case SemanticDirection.INCOMING => {
        val inComingPathUtils = new PathUtils(graphFacade)

        Iterator(startNode)
          .flatMap(startNode =>
            inComingPathUtils.getSingleNodeIncomingPaths(startNode, relationshipFilter)
          )
          .filter(p => endNodeFilter.matches(p.endNode()))
          .flatMap(f => f.pathTriples)
      }
      case SemanticDirection.BOTH => {
        val pathUtils = new PathUtils(graphFacade)

        val startNodes = Iterator(startNode).duplicate
        (startNodes._1
          .flatMap(startNode => pathUtils.getSingleNodeOutgoingPaths(startNode, relationshipFilter))
          .filter(p => endNodeFilter.matches(p.endNode())) ++
          startNodes._2
            .flatMap(endNode => pathUtils.getSingleNodeIncomingPaths(endNode, relationshipFilter))
            .filter(p => endNodeFilter.matches(p.endNode()))).flatMap(f => f.pathTriples)
      }
    }
  }

  def getExpandPathsWithLength(
      startNode: LynxNode,
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
          initExpandInOutStartHop(startNode, relationshipFilter, direction, lowerLimit)

        val hops = getExpandOutGoingPathsWithLength(
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
          initExpandInOutStartHop(startNode, relationshipFilter, direction, lowerLimit)
        val hops = getExpandInComingLengthPaths(
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
        val hopsToLowerLimit = initExpandBothStartHop(startNode, relationshipFilter, lowerLimit)
        val hops = getExpandBothLengthPaths(
          hopsToLowerLimit,
          relationshipFilter,
          startNode,
          endNodeFilter,
          lowerLimit,
          upperLimit
        )
        val r1 = hops.map(hop => hop.paths.map(path => path.pathTriples))
        r1.foldLeft(Seq.empty[Seq[PathTriple]])((a, b) => a ++ b).toIterator
      }
    }
  }

  private def getExpandOutGoingPathsWithLength(
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

  private def initExpandInOutStartHop(
      startNode: LynxNode,
      relationshipFilter: RelationshipFilter,
      direction: SemanticDirection,
      lowerLimit: Int
    ): Seq[GraphHop] = {
    val beginNodes = Iterator(startNode)
    lowerLimit match {
      // 0 means relationship to itself, like (a)-->(a)
      case 0 => {
        val res = beginNodes.map(node => GraphPath(Seq(PathTriple(node, null, node)))).toSeq
        Seq(GraphHop(res))
      }
      case _ => {
        direction match {
          case SemanticDirection.OUTGOING => {
            getExpandOutGoingHopFromOne2Limit(beginNodes, relationshipFilter, lowerLimit)
          }
          case SemanticDirection.INCOMING => {
            getExpandInComingHopFromOne2Limit(beginNodes, relationshipFilter, lowerLimit)
          }
        }
      }
    }
  }

  private def getExpandOutGoingHopFromOne2Limit(
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
  private def getExpandInComingLengthPaths(
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
  private def getExpandInComingHopFromOne2Limit(
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
  private def initExpandBothStartHop(
      startNode: LynxNode,
      relationshipFilter: RelationshipFilter,
      lowerLimit: Int
    ): Seq[GraphHop] = {
    val beginNodes = Iterator(startNode)
    lowerLimit match {
      case 0 => {
        val res = beginNodes.map(node => GraphPath(Seq(PathTriple(node, null, node)))).toSeq
        Seq(GraphHop(res))
      }
      case _ => {
        getExpandBothHopFromOne2Limit(beginNodes, relationshipFilter, lowerLimit)
      }
    }
  }

  private def getExpandBothLengthPaths(
      hopsToLowerLimit: Seq[GraphHop],
      relationshipFilter: RelationshipFilter,
      startNode: LynxNode,
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
        startNode.id == thisPath.pathTriples.head.startNode.id && endNodeFilter.matches(
          thisPath.pathTriples.last.endNode
        )
      )
      filteredResult.append(GraphHop(res))
    })
    filteredResult
  }

  private def getExpandBothHopFromOne2Limit(
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
