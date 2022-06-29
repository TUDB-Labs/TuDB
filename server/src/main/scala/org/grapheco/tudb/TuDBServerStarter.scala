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
    TuInstanceContext.setDataPath(conf.getString("datapath"))
    TuInstanceContext.setPort(conf.getInt("port"))
    TuInstanceContext.setIndexUri(conf.getString("index.uri"))
    serverContext.setDataPath(TuInstanceContext.getDataPath)
    serverContext.setPort(TuInstanceContext.getPort)
    serverContext.setIndexUri(TuInstanceContext.getIndexUri)
    serverContext
  }

}
