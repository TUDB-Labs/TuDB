package org.grapheco.tudb

import com.typesafe.scalalogging.LazyLogging
import com.typesafe.scalalogging.LazyLogging
import io.grpc.Server
import io.grpc.netty.shaded.io.grpc.netty.{NettyServerBuilder => SNettyServerBuilder}
import io.grpc.netty.shaded.io.netty.buffer.ByteBuf
import io.grpc.stub.StreamObserver
import org.grapheco.lynx.lynxrpc.{LynxByteBufFactory, LynxValueSerializer}
import org.grapheco.lynx.types.composite.{LynxList, LynxMap}
import org.grapheco.tudb.facade.GraphFacade
import org.grapheco.tudb.network.Query.QueryResponse
import org.grapheco.tudb.network.{Query, TuQueryServiceGrpc}


/** @Author: Airzihao
  * @Description:
  * @Date: Created at 15:35 2022/4/1
  * @Modified By:
  */
class TuDBServer(bindPort: Int, dbPath: String,indexUri:String)  extends LazyLogging {

  private val _port: Int = bindPort
  private val _server: Server = SNettyServerBuilder
    .forPort(_port)
    .addService(new TuDBQueryService(dbPath,indexUri))
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
  }

  // Caution: Init all the config item in this function.
  private def _initContext(configFile: File) = {
    val props: Properties = new Properties()
    props.load(new FileReader(configFile))

    TuInstanceContext.setDataPath(props.getProperty("datapath"))
    TuInstanceContext.setPort(props.getProperty("port").toInt)
  }

}

