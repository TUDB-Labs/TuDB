package org.grapheco.tudb

import com.typesafe.scalalogging.LazyLogging
import io.grpc.Server
import io.grpc.netty.shaded.io.grpc.netty.{NettyServerBuilder => SNettyServerBuilder}
import org.grapheco.tudb.common.utils.LogUtil
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit

class GraphAPIServer(serverContext: TuDBServerContext) extends LazyLogging {

  val LOGGER = LoggerFactory.getLogger("graph-api-server-info")

  private val _port: Int = serverContext.getPort
  private val _server: Server = SNettyServerBuilder
    .forPort(_port)
    .addService(new TuDBQueryService(serverContext.getDataPath, serverContext.getIndexUri))
    .build()

  def start(): Unit = {
    _server.start()
    LogUtil.info(LOGGER, "Graph API server started successfully")
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        _server.shutdown()
      }
    })
    _server.awaitTermination()
  }

  def shutdown(): Unit = {
    _server.shutdown().awaitTermination(5, TimeUnit.SECONDS)
  }
}
