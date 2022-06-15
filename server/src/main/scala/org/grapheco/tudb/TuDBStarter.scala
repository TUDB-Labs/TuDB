package org.grapheco.tudb

import com.typesafe.scalalogging.LazyLogging

import com.typesafe.config.ConfigFactory

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 14:42 2022/4/19
  * @Modified By:
  */

object TuDBStarter extends LazyLogging {

  def main(args: Array[String]): Unit = {
    /*
      started by script of tudb.sh
     */
    _initContext()

    val server: TuDBServer = new TuDBServer(
      TuInstanceContext.getPort,
      TuInstanceContext.getDataPath
    )
    server.start()
  }

  // Caution: Init all the config item in this function.
  private def _initContext() = {
    val conf = ConfigFactory.load
    TuInstanceContext.setDataPath(conf.getString("datapath"))
    TuInstanceContext.setPort(conf.getInt("port"))
  }
}
