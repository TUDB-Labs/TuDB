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
import org.grapheco.tudb.common.utils.LogUtil
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 15:35 2022/4/1
  * @Modified By:
  */
class TuDBServer(serverContext: TuDBServerContext) extends LazyLogging {

  /** main logger */
  val LOGGER = LoggerFactory.getLogger("server-info")

  private val _port: Int = serverContext.getPort
  private val _server: Server = SNettyServerBuilder
    .forPort(_port)
    .addService(new TuDBQueryService(serverContext.getDataPath, serverContext.getIndexUri))
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
    LogUtil.info(LOGGER, "TuDB server shutdown")
    _server.shutdown().awaitTermination(5, TimeUnit.SECONDS)
  }

}
