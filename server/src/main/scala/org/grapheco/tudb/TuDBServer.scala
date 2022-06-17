package org.grapheco.tudb

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import io.grpc.Server
import io.grpc.netty.shaded.io.grpc.netty.{NettyServerBuilder => SNettyServerBuilder}


/** @Author: Airzihao
 * @Description:
 * @Date: Created at 15:35 2022/4/1
 * @Modified By:
 */
class TuDBServer(bindPort: Int, dbPath: String, indexUri: String) extends LazyLogging {

  private val _port: Int = bindPort
  private val _server: Server = SNettyServerBuilder
    .forPort(_port)
    .addService(new TuDBQueryService(dbPath, indexUri))
    .build()

  def start(): Unit = {
    _server.start()
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

  def main(args: Array[String]): Unit = {
    /*
      started by script of tudb.sh
     */
    _initContext()

    val server: TuDBServer = new TuDBServer(
      TuInstanceContext.getPort,
      TuInstanceContext.getDataPath,
      TuInstanceContext.getIndexUri
    )
    server.start()
  }

  // Caution: Init all the config item in this function.
  private def _initContext() = {
    val conf = ConfigFactory.load
    TuInstanceContext.setDataPath(conf.getString("datapath"))
    TuInstanceContext.setPort(conf.getInt("port"))
    TuInstanceContext.setIndexUri(conf.getString("index.uri"))
  }


}

