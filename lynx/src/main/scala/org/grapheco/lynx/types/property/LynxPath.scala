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

package org.grapheco.lynx.types.property

import org.grapheco.lynx.{LynxType}
import org.grapheco.lynx.graph.PathTriple
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxNode, LynxRelationship}
import org.opencypher.v9_0.util.symbols.CTPath

/** @author:John117
  * @createDate:2022/6/23
  * @description:
  */
case class LynxPath(path: Seq[PathTriple]) extends LynxValue {
  override def value: Any = path

  override def lynxType: LynxType = CTPath

  def startNode(): LynxNode = path.head.startNode

  def endNode(): LynxNode = path.last.endNode

  def lastRelationship(): LynxRelationship = path.last.storedRelation

  def relationships(): Iterator[LynxRelationship] = {
    path.map(f => f.storedRelation).toIterator
  }

  def reverseRelationships(): Iterator[LynxRelationship] = {
    path.reverse.map(f => f.storedRelation).toIterator
  }

  def nodes(): Iterator[LynxNode] = {
    val n = Seq(startNode()) ++ path.map(t => t.endNode)
    n.iterator
  }
  def reverseNodes(): Iterator[LynxNode] = {
    nodes().toSeq.reverse.toIterator
  }
}
