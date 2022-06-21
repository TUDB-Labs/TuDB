package org.grapheco.tudb

import org.grapheco.tudb.common.utils.LogUtil
import org.slf4j.LoggerFactory

import java.io.{File, FileReader}
import java.util.Properties

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
      args(0): tudb.conf file path
     */
    if (args.length != 1) sys.error("Need conf file path.")
    val configFile: File = new File(args(0))
    _initContext(configFile)

    val server: TuDBServer = new TuDBServer(
      TuInstanceContext.getPort,
      TuInstanceContext.getDataPath
    )
    LogUtil.info(LOGGER, "TuDB server is starting,config file is %s", args(0))
    server.start()

  }

  /**
   * Caution: Init all the config item in this function.
   *
   * @param configFile
   */
  private def _initContext(configFile: File) = {
    val props: Properties = new Properties()
    props.load(new FileReader(configFile))

    TuInstanceContext.setDataPath(props.getProperty("datapath"))
    TuInstanceContext.setPort(props.getProperty("port").toInt)
  }

}