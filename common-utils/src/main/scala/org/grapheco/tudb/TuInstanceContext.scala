package org.grapheco.tudb

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 17:50 2022/4/12
  * @Modified By:
  */
object TuInstanceContext extends ContextMap {

  def setDataPath(path: String): Unit = {
    // Todo: check whether the path is illegal.
    // for a new instance, the path should be empty
    // for an old instance, the path should include a description file.
    super.put("dataPath", path)
  }

  def getDataPath: String = super.get[String]("dataPath")

  def setPort(port: Int): Unit = super.put("bindPort", port)

  def getPort: Int = super.get[Int]("bindPort")

  def setIndexUri(indexUri:String)=super.put("indexUri",indexUri)

  def getIndexUri=super.get[String]("indexUri")

}
