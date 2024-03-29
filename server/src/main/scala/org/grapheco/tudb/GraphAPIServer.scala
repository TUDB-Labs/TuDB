// Copyright 2022 The TuDB Authors. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
  TuDBStoreContext.initializeRelationshipStoreAPI(
    s"${outputRoot}/${DBNameMap.relationDB}",
    "default",
    s"${outputRoot}/${DBNameMap.inRelationDB}",
    "default",
    s"${outputRoot}/${DBNameMap.outRelationDB}",
    "default",
    s"${outputRoot}/${DBNameMap.relationLabelDB}",
    "default",
    metaDB
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
    .addService(
      new RelationshipService(
        serverContext.getDataPath,
        serverContext.getIndexUri,
        TuDBStoreContext.getRelationshipAPI
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
