package org.grapheco.lynx.graph

import org.grapheco.lynx.physical.filters.{NodeFilter, RelationshipFilter}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxId, LynxNodeLabel, LynxPropertyKey, LynxRelationshipType}
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
    val allPathTriples = direction match {
      case SemanticDirection.INCOMING => {
        graphModel
          .relationships()
          .filter(pathTriple => relationshipFilter.matches(pathTriple.storedRelation))
          .map(pathTriple => pathTriple.revert)
          .toArray
      }
      case _ => {
        graphModel
          .relationships()
          .filter(pathTriple => relationshipFilter.matches(pathTriple.storedRelation))
          .toArray
      }
    }
    getHops(
      leftNodeFilter,
      rightNodeFilter,
      allPathTriples,
      lowerHop,
      upperHop,
      direction
    )
  }
  private def getHops(
      leftNodeFilter: NodeFilter,
      rightNodeFilter: NodeFilter,
      allPathTriples: Array[PathTriple],
      lowerHop: Int,
      upperHop: Int,
      direction: SemanticDirection
    ): Iterator[Seq[PathTriple]] = {
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

    val filteredResult = ArrayBuffer[Seq[Seq[PathTriple]]]()

    direction match {
      case SemanticDirection.BOTH => {
        bothSearchHelper(lowerHop, upperHop, collectedHopPaths, allPathTriples)
        collectedHopPaths.foreach(hopPaths => {
          val filtered = hopPaths.filter(oneOfPaths => {
            rightNodeFilter.matches(oneOfPaths.last.endNode)
          })
          if (filtered.nonEmpty) filteredResult.append(filtered)
        })
      }
      case _ => {
        inOutHopSearchHelper(upperHop, collectedHopPaths, allPathTriples)
        collectedHopPaths.foreach(hopPaths => {
          val filtered =
            hopPaths.filter(oneOfPaths => rightNodeFilter.matches(oneOfPaths.last.endNode))
          if (filtered.nonEmpty) filteredResult.append(filtered)
        })
      }
    }

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
  private def inOutHopSearchHelper(
      upperHop: Int,
      collectedHopPaths: ArrayBuffer[Seq[Seq[PathTriple]]],
      allPathTriples: Array[PathTriple]
    ): ArrayBuffer[Seq[Seq[PathTriple]]] = {
    for (epoch <- 0 until upperHop) {
      val previousHopPaths = collectedHopPaths(epoch)
      val nextHopPaths = {
        val nextHop = previousHopPaths
          .flatMap(oneOfPreviousHopPaths => {
            val thisPathRelationships = oneOfPreviousHopPaths.map(p => p.storedRelation)
            val nextPaths = allPathTriples
              .filter(pathTriple => pathTriple.startNode == oneOfPreviousHopPaths.last.endNode)
              .filter(pathTriple => !thisPathRelationships.contains(pathTriple.storedRelation)) // circle check
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

  private def bothSearchHelper(
      lowerHop: Int,
      upperHop: Int,
      collectedHopPaths: ArrayBuffer[Seq[Seq[PathTriple]]],
      allPathTriples: Array[PathTriple]
    ): ArrayBuffer[Seq[Seq[PathTriple]]] = {
    for (epoch <- 0 until upperHop) {
      val previousHopPaths = collectedHopPaths(epoch)
      val nextHopPaths = {
        val nextHop = previousHopPaths
          .flatMap(oneOfPreviousHopPaths => {
            val thisPathRelationships = oneOfPreviousHopPaths.map(p => p.storedRelation)
            val nextPaths = allPathTriples
              .filter(pathTriple =>
                pathTriple.startNode == oneOfPreviousHopPaths.last.endNode ||
                  pathTriple.endNode == oneOfPreviousHopPaths.last.endNode
              )
              .filter(pathTriple => !thisPathRelationships.contains(pathTriple.storedRelation)) // circle check
              .toSeq

            if (nextPaths.nonEmpty)
              nextPaths.map(oneOfSinglePath => oneOfPreviousHopPaths ++ Seq(oneOfSinglePath))
            else Seq.empty
          })
          .filter(pathTriples => pathTriples.nonEmpty)

        if (nextHop.isEmpty) return collectedHopPaths
        nextHop
      }

      val noRepeatPaths: ArrayBuffer[Seq[PathTriple]] = ArrayBuffer.empty
      val pathRelIds: ArrayBuffer[Set[LynxId]] = ArrayBuffer.empty
      if (lowerHop != 0) {
        nextHopPaths.foreach(paths => {
          val relIds = paths.map(p => p.storedRelation.id).toSet
          if (!pathRelIds.contains(relIds)) {
            noRepeatPaths.append(paths)
            pathRelIds.append(relIds)
          }
        })
      } else {
        nextHopPaths.foreach(paths => {
          val relIds = paths.drop(1).map(p => p.storedRelation.id).toSet
          if (!pathRelIds.contains(relIds)) {
            noRepeatPaths.append(paths)
            pathRelIds.append(relIds)
          }
        })
      }

      collectedHopPaths.append(noRepeatPaths)
    }
    collectedHopPaths
  }

}
