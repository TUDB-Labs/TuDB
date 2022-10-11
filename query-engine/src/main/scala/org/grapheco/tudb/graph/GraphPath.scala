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

package org.grapheco.tudb.graph

import org.grapheco.lynx.graph.PathTriple
import org.grapheco.lynx.types.structural.{LynxNode, LynxRelationship}

/** @author:John117
  * @createDate:2022/6/27
  * @description:
  */
/** @param pathTriples all paths expanded from a node, each pattern like (a)-[r]-(b) called as a Path
  */
case class GraphPath(pathTriples: Seq[PathTriple]) {

  def length: Int = pathTriples.length

  def revert: GraphPath = GraphPath(pathTriples.map(f => f.revert))

  def isEmpty: Boolean = pathTriples.isEmpty

  def nonEmpty: Boolean = pathTriples.nonEmpty

  def startNode(): LynxNode = pathTriples.head.startNode

  def endNode(): LynxNode = pathTriples.last.endNode

  def lastRelationship(): LynxRelationship = pathTriples.last.storedRelation

  def relationships(): Iterator[LynxRelationship] = {
    pathTriples.map(f => f.storedRelation).toIterator
  }

  def reverseRelationships(): Iterator[LynxRelationship] = {
    pathTriples.reverse.map(f => f.storedRelation).toIterator
  }

  def nodes(): Iterator[LynxNode] = {
    val n = Seq(startNode()) ++ pathTriples.map(t => t.endNode)
    n.iterator
  }
  def reverseNodes(): Iterator[LynxNode] = {
    nodes().toSeq.reverse.toIterator
  }
}
