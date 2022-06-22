package org.grapheco.tudb

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import io.grpc.Server
import io.grpc.netty.shaded.io.grpc.netty.{NettyServerBuilder => SNettyServerBuilder}
import org.grapheco.tudb.common.utils.LogUtil
import org.slf4j.LoggerFactory

/** @Author: Airzihao
 * @Description:
 * @Date: Created at 15:35 2022/4/1
 * @Modified By:
 */
class TuDBServer(bindPort: Int, dbPath: String, indexUri: String) extends LazyLogging {

  /** main logger */
  val LOGGER = LoggerFactory.getLogger("server-info")

  private val _port: Int = bindPort
  private val _server: Server = SNettyServerBuilder
    .forPort(_port)
    .addService(new TuDBQueryService(dbPath, indexUri))
    .build()

  def start(): Unit = {
    _server.start()
    LogUtil.info(LOGGER, "TuDB server started successfully")
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        _server.shutdown()
      }
    })
    _server.awaitTermination()
  }

  def shutdown(): Unit = {
    _server.shutdown()
  }


}

