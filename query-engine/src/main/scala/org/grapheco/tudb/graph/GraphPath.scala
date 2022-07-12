package org.grapheco.tudb.graph

import org.grapheco.lynx.PathTriple
import org.grapheco.lynx.types.structural.{LynxNode, LynxRelationship}

/** @author:John117
  * @createDate:2022/6/27
  * @description:
  */
/** @param pathTriples A kind of path, each pattern like (a)-[r]-(b) called as a Path
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
