package org.grapheco.tudb

import com.typesafe.scalalogging.LazyLogging
import io.grpc.Server
import io.grpc.netty.shaded.io.grpc.netty.{NettyServerBuilder => SNettyServerBuilder}
import org.grapheco.tudb.common.utils.LogUtil
import org.slf4j.{Logger, LoggerFactory}

import java.io.{File, FileReader}
import java.util.Properties


/** @Author: Airzihao
 * @Description:
 * @Date: Created at 15:35 2022/4/1
 * @Modified By:
 */
class TuDBServer(bindPort: Int, dbPath: String) extends LazyLogging {

  val LOGGER = LoggerFactory.getLogger(this.getClass)

  private val _port: Int = bindPort
  private val _server: Server = SNettyServerBuilder
    .forPort(_port)
    .addService(new TuDBQueryService(dbPath))
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
      args(0): tudb.conf file path
     */
    if (args.length != 1) sys.error("Need conf file path.")
    val configFile: File = new File(args(0))
    _initContext(configFile)

    val server: TuDBServer = new TuDBServer(
      TuInstanceContext.getPort,
      TuInstanceContext.getDataPath
    )
    server.start()
    LogUtil.info(LOGGER, "Server started success!")
  }

  // Caution: Init all the config item in this function.
  private def _initContext(configFile: File) = {
    val props: Properties = new Properties()
    props.load(new FileReader(configFile))

    TuInstanceContext.setDataPath(props.getProperty("datapath"))
    TuInstanceContext.setPort(props.getProperty("port").toInt)
  }

}

