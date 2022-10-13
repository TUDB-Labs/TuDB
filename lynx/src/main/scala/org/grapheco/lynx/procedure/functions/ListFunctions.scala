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

package org.grapheco.lynx.procedure.functions

import org.grapheco.lynx.LynxProcedureException
import org.grapheco.lynx.func.LynxProcedure
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.{LynxList, LynxMap}
import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.structural.{LynxNode, LynxRelationship}

/** @ClassName List functions return lists of things — nodes in a path, and so on.
  * @Description TODO
  * @Author huchuan
  * @Date 2022/4/20
  * @Version 0.1
  */
class ListFunctions {

  /** Returns a list containing the string representations
    * for all the property names of a node, relationship, or map.
    * @param x A node, relationship, or map
    * @return property names
    */
  @LynxProcedure(name = "keys")
  def keys(args: Seq[LynxValue]): List[String] = args.head match {
    case n: LynxNode         => n.keys.toList.map(_.value)
    case r: LynxRelationship => r.keys.toList.map(_.value)
    case m: LynxMap          => m.value.keys.toList
    case _                   => throw LynxProcedureException("keys() can only used on node, relationship and map.")
  }

  /** Returns a list containing the string representations for all the labels of a node.
    * @param x The node
    * @return labels
    */
  @LynxProcedure(name = "labels")
  def labels(args: Seq[LynxNode]): Seq[String] = args.head.labels.map(_.value)

  /** Returns a list containing all the nodes in a path.
    * @param inputs path
    * @return nodes
    */
  @LynxProcedure(name = "nodes")
  def nodes(args: Seq[LynxList]): List[LynxNode] = {
    def fetchNodeFromList(args: Seq[LynxList]): LynxNode = {
      args.head.value.filter(item => item.isInstanceOf[LynxNode]).head.asInstanceOf[LynxNode]
    }

    def fetchListFromList(args: Seq[LynxList]): LynxList = {
      args.head.value.filter(item => item.isInstanceOf[LynxList]).head.asInstanceOf[LynxList]
    }

    val list = fetchListFromList(args)
    if (list.value.nonEmpty) List(fetchNodeFromList(args)) ++ nodes(Seq(fetchListFromList(args)))
    else List(fetchNodeFromList(args))
  }

  /** Returns a list comprising all integer values within a specified range. TODO
    * @param inputs
    * @return
    */

  @LynxProcedure(name = "range")
  def range(args: Seq[LynxInteger]): LynxList = {
    args.size match {
      case 2 => {
        val start = args.head
        val end = args.last
        LynxList((start.value to end.value).toList map LynxInteger)
      }
      case 3 => {
        val start = args.head
        val end = args(1)
        val step = args.last
        LynxList((start.value to end.value by step.value).toList map LynxInteger)
      }
    }
  }

  /** Returns a list containing all the relationships in a path.
    * @param inputs path
    * @return relationships
    */
  @LynxProcedure(name = "relationships")
  def relationships(args: Seq[LynxList]): List[LynxRelationship] = {
    val list: LynxList = args.head.value.tail.head.asInstanceOf[LynxList]
    list.value
      .filter(value => value.isInstanceOf[LynxRelationship])
      .asInstanceOf[List[LynxRelationship]]
      .reverse
  }

  // TODO : reverse() tail()
}
