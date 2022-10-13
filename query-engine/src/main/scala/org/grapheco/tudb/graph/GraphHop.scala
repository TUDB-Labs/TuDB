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

/** @author:John117
  * @createDate:2022/6/28
  * @description:
  */
/** @param paths this hop's all paths
  *
  *    example:
  *
  *              A
  *          /   |   \
  *        B     C    D
  *       /      |
  *      E       F
  *     |
  *     G
  *
  *     each pattern like [A->B], [A->B->E], [A->B->E->G] called as a path
  *     hop is a collection of paths.
  *
  *     A->B, A->C, A->D then GraphHop( Seq(A->B, A->C, A->D ) ) called one hop
  *     A->B-E, A->C->F then GraphHop(Seq( A->B->E, A->C->F  ) ) called two hop
  *     A->B->E->G then GraphHop( Seq( A->B->E->G )) called three hop
  */
case class GraphHop(paths: Seq[GraphPath]) {}
