package org.grapheco.tudb.client

import com.typesafe.scalalogging.LazyLogging
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import org.grapheco.tudb.TuDBJsonTool.objectMapper
import org.grapheco.tudb.core.{Core, NodeServiceGrpc}
import org.grapheco.tudb.network.{Query, TuQueryServiceGrpc}
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters.asScalaIteratorConverter

class GraphAPIClient(host: String, port: Int) extends LazyLogging {

  val LOGGER = LoggerFactory.getLogger("graph-api-client-info")
  val channel =
    NettyChannelBuilder.forAddress(host, port).usePlaintext().build();
  val nodeServiceBlockingStub = NodeServiceGrpc.newBlockingStub(channel)

  def getNode(name: String): Core.Node = {
    val request: Core.NodeGetRequest =
      Core.NodeGetRequest.newBuilder().setName(name).build()
    val response = nodeServiceBlockingStub.getNode(request)
    if (response.getStatus.getExitCode == 0) {
      response.getNode
    } else {
      logger.info(f"Failed to get node $name: ${response.getStatus.getMessage}")
      new Core.Node()
    }
  }

  def deleteNode(name: String) {
    val request: Core.NodeDeleteRequest =
      Core.NodeDeleteRequest.newBuilder().setName(name).build()
    val response = nodeServiceBlockingStub.deleteNode(request)
    if (response.getStatus.getExitCode == 0) {
      logger.info(f"Successfully deleted node $name")
    } else {
      logger.info(f"Failed to delete node $name: ${response.getStatus.getMessage}")
    }
  }

  def shutdown(): Unit = {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }

}
