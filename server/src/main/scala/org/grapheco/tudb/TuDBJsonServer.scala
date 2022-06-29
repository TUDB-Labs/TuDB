/** Copyright (c) 2022 PandaDB * */
package org.grapheco.tudb

import com.typesafe.scalalogging.LazyLogging
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.logging.{LogLevel, LoggingHandler}
import org.grapheco.tudb.common.utils.LogUtil
import org.slf4j.LoggerFactory

/** @Author: huanglin
  * @Description:
  * @Date: Created at 2022-6-29
  */
class TuDBJsonServer(serverContext: TuDBServerContext) extends LazyLogging {

  /** main logger */
  val LOGGER = LoggerFactory.getLogger("json-server-info")
  private val bossGroup = new NioEventLoopGroup(1)
  private val workerGroup = new NioEventLoopGroup()
  private val db=GraphDatabaseBuilder.newEmbeddedDatabase(serverContext.getDataPath,serverContext.getIndexUri)
  private val _server: ServerBootstrap = new ServerBootstrap()
    .group(bossGroup, workerGroup)
    .channel(classOf[NioServerSocketChannel])
    .handler(new LoggingHandler(LogLevel.INFO))
    .childHandler(new TuDBQueryServerHandler(db))

  def start(): Unit = {
    LogUtil.info(LOGGER, "TuDB json server started successfully")
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        shutdown()
      }
    })
    try {
      val f = _server.bind(serverContext.getPort).sync()
      f.channel().closeFuture().sync()
    } finally {
      shutdown()
    }
  }

  def shutdown(): Unit = {
    bossGroup.shutdownGracefully()
    workerGroup.shutdownGracefully()
  }

}
