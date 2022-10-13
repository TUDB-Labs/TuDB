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

package org.grapheco.lynx.graph

import org.grapheco.lynx.types.structural.{LynxNode, LynxRelationship}

/**
  *@description:
  */
/** A triplet of path.
  * @param startNode start node
  * @param storedRelation the relation from start-node to end-node
  * @param endNode end node
  * @param reverse If true, it means it is in reverse order
  */
case class PathTriple(
    startNode: LynxNode,
    storedRelation: LynxRelationship,
    endNode: LynxNode,
    reverse: Boolean = false) {
  def revert: PathTriple = PathTriple(endNode, storedRelation, startNode, !reverse)
}
