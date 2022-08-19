package org.grapheco.tudb

import com.typesafe.scalalogging.LazyLogging
import io.grpc.Server
import io.grpc.netty.shaded.io.grpc.netty.{NettyServerBuilder => SNettyServerBuilder}
import org.apache.commons.io.FileUtils
import org.grapheco.tudb.common.utils.LogUtil
import org.grapheco.tudb.store.meta.DBNameMap
import org.grapheco.tudb.store.storage.{KeyValueDB, RocksDBStorage}
import org.grapheco.tudb.test.TestUtils
import org.slf4j.LoggerFactory

import java.io.File
import java.util.concurrent.TimeUnit

class GraphAPIServer(serverContext: TuDBServerContext) extends LazyLogging {

  val LOGGER = LoggerFactory.getLogger("graph-api-server-info")

  val outputRoot: String =
    s"${TestUtils.getModuleRootPath}/testOutput/nodeStoreTest"
  TuDBInstanceContext.setDataPath(s"$outputRoot")
  val metaDB: KeyValueDB =
    RocksDBStorage.getDB(s"$outputRoot/${DBNameMap.nodeMetaDB}")
  TuDBStoreContext.initializeNodeStoreAPI(
    s"$outputRoot/${DBNameMap.nodeDB}",
    "default",
    s"$outputRoot/${DBNameMap.nodeLabelDB}",
    "default",
    metaDB,
    "tudb://index?type=dummy",
    outputRoot
  )

  private val _port: Int = serverContext.getPort
  private val _server: Server = SNettyServerBuilder
    .forPort(_port)
    .addService(
      new NodeService(
        serverContext.getDataPath,
        serverContext.getIndexUri,
        TuDBStoreContext.getNodeStoreAPI
      )
    )
    .build()

  def start(): Unit = {
    _server.start()
    LogUtil.info(LOGGER, "Graph API server started successfully")
    val file: File = new File(s"$outputRoot")
    if (file.exists()) FileUtils.deleteDirectory(file)
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        _server.shutdown()
      }
    })
    _server.awaitTermination()
  }

  def shutdown(): Unit = {
    TuDBStoreContext.getNodeStoreAPI.close()
    metaDB.close()
    val file: File = new File(s"$outputRoot")
    if (file.exists()) FileUtils.deleteDirectory(file)
    _server.shutdown().awaitTermination(5, TimeUnit.SECONDS)
  }
}
