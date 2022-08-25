package org.grapheco.tudb.client

import com.typesafe.scalalogging.LazyLogging
import io.grpc.ManagedChannel
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import org.grapheco.tudb.core.{Core, NodeServiceGrpc}
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._

class GraphAPIClient(host: String, port: Int) extends LazyLogging {

  val LOGGER = LoggerFactory.getLogger("graph-api-client-info")
  var channel: ManagedChannel = null
  var nodeServiceBlockingStub: NodeServiceGrpc.NodeServiceBlockingStub = null

  try {
    channel = NettyChannelBuilder.forAddress(host, port).usePlaintext().build()
    nodeServiceBlockingStub = NodeServiceGrpc.newBlockingStub(channel)
  } catch {
    case _: Throwable =>
      if (channel != null) {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS)
      }
  }

  def createNode(node: Core.Node): Core.Node = {
    val request: Core.NodeCreateRequest =
      Core.NodeCreateRequest.newBuilder().setNode(node).build()
    val response = nodeServiceBlockingStub.createNode(request)
    if (response.getStatus.getExitCode == 0) {
      response.getNode
    } else {
      logger.info(f"Failed to create node ${node.getName}: ${response.getStatus.getMessage}")
      null
    }
  }

  def getNode(id: Long): Core.Node = {
    val request: Core.NodeGetRequest =
      Core.NodeGetRequest.newBuilder().setNodeId(id).build()
    val response = nodeServiceBlockingStub.getNode(request)
    // TODO: Need to check whether the node is null. response.hasNode
    if (response.getStatus.getExitCode == 0) {
      response.getNode
    } else {
      logger.info(f"Failed to get node $id: ${response.getStatus.getMessage}")
      null
    }
  }

  def deleteNode(id: Long) {
    val request: Core.NodeDeleteRequest =
      Core.NodeDeleteRequest.newBuilder().setNodeId(id).build()
    val response = nodeServiceBlockingStub.deleteNode(request)
    if (response.getStatus.getExitCode == 0) {
      logger.info(f"Successfully deleted node $id")
    } else {
      logger.info(f"Failed to delete node $id: ${response.getStatus.getMessage}")
    }
  }

  def listNodes(): List[Core.Node] = {
    val request: Core.NodeListRequest =
      Core.NodeListRequest.newBuilder().build()
    val response = nodeServiceBlockingStub.listNodes(request)
    if (response.getStatus.getExitCode == 0) {
      response.getNodesList.asScala.toList
    } else {
      logger.info(f"Failed to list nodes")
      null
    }
  }

  def shutdown(): Unit = {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }
}
