package org.grapheco.lynx.graph

import org.grapheco.lynx.physical.filters.{NodeFilter, RelationshipFilter}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey, LynxRelationshipType}
import org.opencypher.v9_0.expressions.SemanticDirection

import scala.collection.mutable.ArrayBuffer

/**
  *@description:
  */
case class GraphModelHelper(graphModel: GraphModel) {
  /*
    Operations of indexes
   */
  def createIndex(labelName: String, properties: Set[String]): Unit =
    this.graphModel.indexManager
      .createIndex(Index(LynxNodeLabel(labelName), properties.map(LynxPropertyKey)))

  def dropIndex(labelName: String, properties: Set[String]): Unit =
    this.graphModel.indexManager
      .dropIndex(Index(LynxNodeLabel(labelName), properties.map(LynxPropertyKey)))

  def indexes: Array[(String, Set[String])] =
    this.graphModel.indexManager.indexes.map {
      case Index(label, properties) =>
        (label.value, properties.map(_.value))
    }

  /*
    Operations of estimating count.
   */
  def estimateNodeLabel(labelName: String): Long =
    this.graphModel.statistics.numNodeByLabel(LynxNodeLabel(labelName))

  def estimateNodeProperty(labelName: String, propertyName: String, value: AnyRef): Long =
    this.graphModel.statistics
      .numNodeByProperty(LynxNodeLabel(labelName), LynxPropertyKey(propertyName), LynxValue(value))

  def estimateRelationship(relType: String): Long =
    this.graphModel.statistics.numRelationshipByType(LynxRelationshipType(relType))

  def getPathWithLength(
      leftNodeFilter: NodeFilter,
      relationshipFilter: RelationshipFilter,
      rightNodeFilter: NodeFilter,
      lowerHop: Int,
      upperHop: Int,
      direction: SemanticDirection
    ): Iterator[Seq[PathTriple]] = {
    // naive implementation
    direction match {
      case SemanticDirection.OUTGOING => {
        getOutgoingPathWithLength(
          leftNodeFilter,
          relationshipFilter,
          rightNodeFilter,
          lowerHop,
          upperHop
        )
      }
      case SemanticDirection.INCOMING => { ??? }
      case SemanticDirection.BOTH     => { ??? }
    }
  }
  private def getOutgoingPathWithLength(
      leftNodeFilter: NodeFilter,
      relationshipFilter: RelationshipFilter,
      rightNodeFilter: NodeFilter,
      lowerHop: Int,
      upperHop: Int
    ): Iterator[Seq[PathTriple]] = {
    val allPathTriples = graphModel
      .relationships()
      .filter(pathTriple => relationshipFilter.matches(pathTriple.storedRelation))
      .toArray
    val collectedHopPaths = ArrayBuffer[Seq[Seq[PathTriple]]]()

    if (lowerHop == 0) {
      val zeroPaths = graphModel
        .nodes(leftNodeFilter)
        .map(node => PathTriple(node, null, node))
        .toSeq
      collectedHopPaths.append(zeroPaths.map(pathTriple => Seq(pathTriple)))
    } else {
      val firstHopPaths = allPathTriples
        .filter(pathTriple => leftNodeFilter.matches(pathTriple.startNode))
        .map(pathTriple => Seq(pathTriple))
      collectedHopPaths.append(firstHopPaths)
    }
    outgoingHopSearchHelper(upperHop, collectedHopPaths, allPathTriples)

    val filteredResult = ArrayBuffer[Seq[Seq[PathTriple]]]()
    collectedHopPaths.foreach(hopPaths => {
      val filtered = hopPaths.filter(oneOfPaths => rightNodeFilter.matches(oneOfPaths.last.endNode))
      if (filtered.nonEmpty) filteredResult.append(filtered)
    })

    if (lowerHop == 0) {
      filteredResult
        .slice(lowerHop, upperHop + 1)
        .flatten
        .toIterator
    } else {
      filteredResult
        .slice(lowerHop - 1, upperHop)
        .flatten
        .toIterator
    }
  }
  private def outgoingHopSearchHelper(
      upperHop: Int,
      collectedHopPaths: ArrayBuffer[Seq[Seq[PathTriple]]],
      allPathTriples: Array[PathTriple]
    ): ArrayBuffer[Seq[Seq[PathTriple]]] = {
    for (epoch <- 0 until upperHop) {
      val previousHopPaths = collectedHopPaths(epoch)
      val nextHopPaths = {
        val nextHop = previousHopPaths
          .flatMap(oneOfPreviousHopPaths => {
            val nextPaths = allPathTriples
              .filter(pathTriple => pathTriple.startNode == oneOfPreviousHopPaths.last.endNode)
              .filter(pathTriple =>
                !oneOfPreviousHopPaths
                  .map(p => p.storedRelation)
                  .contains(pathTriple.storedRelation)
              ) // circle check
              .toSeq

            if (nextPaths.nonEmpty)
              nextPaths.map(oneOfSinglePath => oneOfPreviousHopPaths ++ Seq(oneOfSinglePath))
            else Seq.empty
          })
          .filter(pathTriples => pathTriples.nonEmpty)

        if (nextHop.isEmpty) return collectedHopPaths
        nextHop
      }
      collectedHopPaths.append(nextHopPaths)
    }
    collectedHopPaths
  }

}
