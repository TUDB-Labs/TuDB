package org.grapheco.tudb.ldbc.performance

import com.typesafe.scalalogging.LazyLogging
import org.grapheco.tudb.exception.{ClientException, TuDBError}
import org.grapheco.tudb.ldbc.performance.Tools.printTimeCalculateResult
import org.grapheco.tudb.store.node.{NodeStoreAPI, StoredNodeWithProperty}

import java.util.concurrent.TimeUnit
import scala.collection.mutable.ArrayBuffer

/** @program: TuDB-Embedded
  * @description:
  * @author: LiamGao
  * @create: 2022-03-24 17:50
  */
class NodePerformance(nodeStore: NodeStoreAPI, testScanAllNodeData: Boolean) extends LazyLogging {
  private var startTime: Long = _
  private val costArray: ArrayBuffer[Long] = ArrayBuffer.empty

  def run(): Unit = {
    val nodes = prepareNodesData()

    if (testScanAllNodeData) {
      logger.info(
        "+++++++++++++++++++++++++++++ Start [Scan All Nodes] Test +++++++++++++++++++++++++++++"
      )
      startTime = System.nanoTime()
      val size = nodeStore.allNodes().size
      val cost = System.nanoTime() - startTime
      logger.info(
        s"scan all $size nodes, cost ${cost} us (${TimeUnit.NANOSECONDS.toMillis(cost)} ms)"
      )
      println()
    }
    logger.info(
      "+++++++++++++++++++++++++++++ Start [GetNodeById] Test +++++++++++++++++++++++++++++"
    )
    getNodeByIdTest(nodes)

    logger.info(
      "+++++++++++++++++++++++++++++ Start [Set And Remove Property] Test +++++++++++++++++++++++++++++"
    )
    updateNodeTest(nodes)

    logger.info(
      "+++++++++++++++++++++++++++++  Start [Delete And Add Node] Test +++++++++++++++++++++++++++++"
    )
    addAndDeleteNodeTest(nodes)
  }

  private def prepareNodesData(): Array[StoredNodeWithProperty] = {
    val labels = nodeStore.allLabels()
    logger.info(s"all node labels: [${labels.mkString(",")}]")
    val nodes = labels.map(label => {
      val labelId = nodeStore.getLabelId(label)
      if (labelId.isDefined) {
        nodeStore.getNodesByLabel(labelId.get).next()
      } else throw new ClientException(TuDBError.CLIENT_ERROR, s"no such label: $label")
    })
    nodes
  }

  private def getNodeByIdTest(nodes: Array[StoredNodeWithProperty]): Unit = {
    nodes.foreach(node => {
      testTemplate(() => nodeStore.getNodeById(node.id))
    })
    printTimeCalculateResult(costArray.toArray, "getNodeById")
    costArray.clear()
    println()
  }

  private def addAndDeleteNodeTest(nodes: Array[StoredNodeWithProperty]): Unit = {
    nodes.foreach(node => {
      testTemplate(() => nodeStore.deleteNode(node.id))
    })
    printTimeCalculateResult(costArray.toArray, "deleteNode")
    costArray.clear()
    println()

    nodes.foreach(node => {
      testTemplate(() => nodeStore.addNode(node))
    })
    printTimeCalculateResult(costArray.toArray, "addNode")
    costArray.clear()
    println()
  }

  private def updateNodeTest(nodes: Array[StoredNodeWithProperty]): Unit = {
    nodes.foreach(node => {
      testTemplate(() => nodeStore.nodeSetProperty(node.id, 233, "test property set"))
    })
    printTimeCalculateResult(costArray.toArray, "nodeSetProperty")
    costArray.clear()
    println()

    nodes.foreach(node => {
      testTemplate(() => nodeStore.nodeRemoveProperty(node.id, 233))
    })
    printTimeCalculateResult(costArray.toArray, "nodeRemoveProperty")
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
