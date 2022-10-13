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

import com.typesafe.config.ConfigFactory
import org.grapheco.tudb.common.utils.LogUtil
import org.slf4j.LoggerFactory

/**
  * the starter of TuDB
  *
  * @author : johnny
  * @date : 2022/6/20
  * */
object TuDBServerStarter {

  /** main logger */
  val LOGGER = LoggerFactory.getLogger("server-info")

  /**
    * main method of TuDB
    * run this method will start local instance of TuDB
    *
    * @param args the absolut path of tudb.properties file
    */
  def main(args: Array[String]): Unit = {
    /*
      started by script of tudb.sh
     */
    val serverContext = _initContext()

    val server: TuDBServer = new TuDBServer(serverContext)
//    LogUtil.info(LOGGER, "TuDB server is starting,config file is %s", args(0))
    server.start()

  }

  /**
    * Caution: Init all the config item in this function.
    *
    */
  private def _initContext(): TuDBServerContext = {
    val conf = ConfigFactory.load
    val serverContext = new TuDBServerContext()
    TuDBInstanceContext.setDataPath(conf.getString("datapath"))
    TuDBInstanceContext.setPort(conf.getInt("port"))
    TuDBInstanceContext.setIndexUri(conf.getString("index.uri"))
    serverContext.setDataPath(TuDBInstanceContext.getDataPath)
    serverContext.setPort(TuDBInstanceContext.getPort)
    serverContext.setIndexUri(TuDBInstanceContext.getIndexUri)
    serverContext
  }

}
