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

  def multipleHopSearch(
      leftNodeFilter: NodeFilter,
      relationshipFilter: RelationshipFilter,
      rightNodeFilter: NodeFilter,
      lowerHop: Int,
      upperHop: Int,
      direction: SemanticDirection
    ): Iterator[Seq[PathTriple]] = {
    // naive implementation
    val allPathTriples = graphModel
      .relationships()
      .filter(pathTriple => relationshipFilter.matches(pathTriple.storedRelation))
      .toArray

    hopSearchHelper(
      leftNodeFilter,
      rightNodeFilter,
      allPathTriples,
      lowerHop,
      upperHop,
      direction
    )
  }

  /*
      Calculate all the hops data from zero hop to upper hop, then slice the range of hops we need.
      The difference of outgoing and incoming is the position of leftNodeFilter and rightNodeFilter.
      Both direction need to check the expand pathTriple's left and right node.
   */
  private def hopSearchHelper(
      leftNodeFilter: NodeFilter,
      rightNodeFilter: NodeFilter,
      allPathTriples: Array[PathTriple],
      lowerHop: Int,
      upperHop: Int,
      direction: SemanticDirection
    ): Iterator[Seq[PathTriple]] = {
    val collectedHopPaths = ArrayBuffer[Seq[Seq[PathTriple]]]()

    if (lowerHop == 0) {
      val zeroPaths = {
        direction match {
          case SemanticDirection.INCOMING => {
            graphModel
              .nodes(rightNodeFilter)
              .map(node => PathTriple(node, null, node))
              .toSeq
          }
          case _ => {
            graphModel
              .nodes(leftNodeFilter)
              .map(node => PathTriple(node, null, node))
              .toSeq
          }
        }
      }
      collectedHopPaths.append(zeroPaths.map(pathTriple => Seq(pathTriple)))
    } else {
      val firstHopPaths = {
        direction match {
          case SemanticDirection.INCOMING => {
            allPathTriples
              .filter(pathTriple => rightNodeFilter.matches(pathTriple.startNode))
              .map(pathTriple => Seq(pathTriple))
          }
          case _ => {
            allPathTriples
              .filter(pathTriple => leftNodeFilter.matches(pathTriple.startNode))
              .map(pathTriple => Seq(pathTriple))
          }
        }
      }
      collectedHopPaths.append(firstHopPaths)
    }
    val filteredResult = ArrayBuffer[Seq[Seq[PathTriple]]]()

    direction match {
      case SemanticDirection.BOTH => {
        bothDirectionHopSearchHelper(lowerHop, upperHop, collectedHopPaths, allPathTriples)
        collectedHopPaths.foreach(hopPaths => {
          val filtered = hopPaths.filter(oneOfPaths => {
            rightNodeFilter.matches(oneOfPaths.last.endNode) ||
              rightNodeFilter.matches(oneOfPaths.last.startNode)
          })
          if (filtered.nonEmpty) filteredResult.append(filtered)
        })
      }
      case _ => {
        inAndOutDirectionHopSearchHelper(upperHop, collectedHopPaths, allPathTriples)
        collectedHopPaths.foreach(hopPaths => {
          val filtered = {
            direction match {
              case SemanticDirection.INCOMING => {
                hopPaths.filter(oneOfPaths => leftNodeFilter.matches(oneOfPaths.last.endNode))
              }
              case SemanticDirection.OUTGOING => {
                hopPaths.filter(oneOfPaths => rightNodeFilter.matches(oneOfPaths.last.endNode))
              }
            }
          }
          if (filtered.nonEmpty) filteredResult.append(filtered)
        })
      }
    }

    // slice hops.
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

  private def inAndOutDirectionHopSearchHelper(
      upperHop: Int,
      collectedHopPaths: ArrayBuffer[Seq[Seq[PathTriple]]],
      allPathTriples: Array[PathTriple]
    ): ArrayBuffer[Seq[Seq[PathTriple]]] = {
    for (epoch <- 0 until upperHop) {
      val previousHopPaths = collectedHopPaths(epoch)
      val nextHopPaths = {
        val nextHop = previousHopPaths
          .flatMap(oneOfPreviousHopPaths => {
            val relationshipsOfCurrentPaths = oneOfPreviousHopPaths.map(p => p.storedRelation)
            val lastNodeOfCurrentPaths = oneOfPreviousHopPaths.last.endNode

            val nextPaths = allPathTriples
              .filter(pathTriple => pathTriple.startNode == lastNodeOfCurrentPaths)
              .filter(pathTriple => !relationshipsOfCurrentPaths.contains(pathTriple.storedRelation)
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

  /*
      Both need to check the pathTriple's left node and right node.
   */
  private def bothDirectionHopSearchHelper(
      lowerHop: Int,
      upperHop: Int,
      collectedHopPaths: ArrayBuffer[Seq[Seq[PathTriple]]],
      allPathTriples: Array[PathTriple]
    ): ArrayBuffer[Seq[Seq[PathTriple]]] = {
    for (epoch <- 0 until upperHop) {
      val previousHopPaths = collectedHopPaths(epoch)
      val nextHopPaths = {
        val nextHop =
          previousHopPaths
            .flatMap(oneOfPreviousHopPaths => {
              val relationshipsOfCurrentPath = oneOfPreviousHopPaths.map(p => p.storedRelation)
              val lastNodeOfCurrentPaths = oneOfPreviousHopPaths.last.endNode

              val nextPaths = allPathTriples
                .filter(pathTriple =>
                  pathTriple.startNode == lastNodeOfCurrentPaths ||
                    pathTriple.endNode == lastNodeOfCurrentPaths
                )
                .filter(pathTriple =>
                  !relationshipsOfCurrentPath.contains(pathTriple.storedRelation)
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

      // we should remove the repeated situation like [1 --> 3 , 4 --> 3] and [4 --> 3, 1 --> 3].
      val noRepeatPaths: ArrayBuffer[Seq[PathTriple]] = ArrayBuffer.empty
      val pathRelIds: ArrayBuffer[Set[LynxId]] = ArrayBuffer.empty
      nextHopPaths.foreach(paths => {
        val relIds = {
          if (lowerHop != 0) paths.map(p => p.storedRelation.id).toSet
          // remove the zero path cause it hasn't relationship.
          else paths.drop(1).map(p => p.storedRelation.id).toSet
        }
        if (!pathRelIds.contains(relIds)) {
          noRepeatPaths.append(paths)
          pathRelIds.append(relIds)
        }
      })
      collectedHopPaths.append(noRepeatPaths)
    }
    collectedHopPaths
  }

}
