package org.grapheco.tudb.ldbc.performance

import com.typesafe.scalalogging.LazyLogging
import org.grapheco.tudb.exception.{ClientException, TuDBError}
import org.grapheco.tudb.ldbc.performance.Tools.printTimeCalculateResult
import org.grapheco.tudb.store.relationship.{RelationshipStoreAPI, StoredRelationshipWithProperty}

import scala.collection.mutable.ArrayBuffer

/** @program: TuDB-Embedded
  * @description:
  * @author: LiamGao
  * @create: 2022-03-24 17:51
  */
class RelationPerformance(relationshipStore: RelationshipStoreAPI) extends LazyLogging {
  private var startTime: Long = _
  private val costArray: ArrayBuffer[Long] = ArrayBuffer.empty

  def run(): Unit = {
    val rels = prepareRelationData()
    logger.info("+++++++++++Start [findInRelationTest] Test+++++++++++")
    findInRelationTest(rels)

    logger.info("+++++++++++Start [getRelationByIdTest] Test+++++++++++")
    getRelationByIdTest(rels)

    logger.info("+++++++++++Start [updateRelationTest] Test+++++++++++")
    updateRelationTest(rels)

    logger.info("+++++++++++Start [addAndDeleteRelation] Test+++++++++++")
    addAndDeleteRelation(rels)
  }

  private def prepareRelationData(): Array[StoredRelationshipWithProperty] = {
    val _relationLabels = relationshipStore.allRelationTypes()
    logger.info(s"all relation types: [${_relationLabels.mkString(",")}]")
    val testInRelationData: ArrayBuffer[StoredRelationshipWithProperty] =
      ArrayBuffer.empty
    _relationLabels.foreach(rType => {
      val tId = relationshipStore.getRelationTypeId(rType)
      if (tId.isDefined) {
        val rId = relationshipStore.getRelationIdsByRelationType(tId.get).next()
        val r = relationshipStore.getRelationById(rId)
        testInRelationData.append(r.get)
      } else throw new ClientException(TuDBError.CLIENT_ERROR, s"no such relation type: $rType")
    })
    testInRelationData.toArray
  }

  private def getRelationByIdTest(
      data: Array[StoredRelationshipWithProperty]
    ): Unit = {
    data.foreach(r => {
      testTemplate(() => relationshipStore.getRelationById(r.id).get)
    })
    printTimeCalculateResult(costArray.toArray, "getRelationById")
    costArray.clear()
    println()
  }
  private def findInRelationTest(
      data: Array[StoredRelationshipWithProperty]
    ): Unit = {
    data.foreach(r => {
      testTemplate(() => {
        val res = relationshipStore
          .findInRelations(r.to, Option(r.typeId))
          .slice(0, 10)
          .toArray
      })
    })
    printTimeCalculateResult(costArray.toArray, "findInRelations")
    costArray.clear()
    println()
  }
  private def updateRelationTest(
      data: Array[StoredRelationshipWithProperty]
    ): Unit = {
    data.foreach(r => {
      testTemplate(() =>
        relationshipStore.relationSetProperty(
          r.id,
          233,
          "test relation property set"
        )
      )
    })
    printTimeCalculateResult(costArray.toArray, "relationSetProperty")
    costArray.clear()
    println()

    data.foreach(r => {
      testTemplate(() => relationshipStore.relationRemoveProperty(r.id, 233))
    })
    printTimeCalculateResult(costArray.toArray, "relationRemoveProperty")
    costArray.clear()
    println()
  }

  private def addAndDeleteRelation(
      data: Array[StoredRelationshipWithProperty]
    ): Unit = {
    data.foreach(r => {
      testTemplate(() => relationshipStore.deleteRelation(r.id))
    })
    printTimeCalculateResult(costArray.toArray, "deleteRelation")
    costArray.clear()
    println()

    data.foreach(r => {
      testTemplate(() => relationshipStore.addRelation(r))
    })
    printTimeCalculateResult(costArray.toArray, "addRelation")
    costArray.clear()
    println()
  }

  private def testTemplate(f: () => Unit): Unit = {
    startTime = System.nanoTime()
    f()
    val cost = System.nanoTime() - startTime
    costArray.append(cost)
  }
}
