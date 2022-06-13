package org.grapheco.tudb

import com.typesafe.scalalogging.LazyLogging

import java.io.{File, FileReader}
import java.util.Properties

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 14:42 2022/4/19
  * @Modified By:
  */

object TuDBStarter extends LazyLogging {

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
