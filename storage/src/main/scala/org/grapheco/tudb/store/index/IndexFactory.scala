/** Copyright (c) 2022 TuDB * */
package org.grapheco.tudb.store.index

import com.typesafe.scalalogging.StrictLogging

import java.net.URI

/** @author: huanglin
  * @createDate: 2022-06-20 17:19:08
  * @description: this is the index engine create  factory.
  */
object IndexFactory extends StrictLogging {

  /** create index engine by uri
    * index engine url
    * memory://{any}  use hashmap storage index data
    * es://ip:port   use  es (Elasticsearch) storage index data,ip:port is es service address
    * db://{path}  use rocksdb storage index data ,path is rocksdb data storage location
    * empty is empty implement ,  use this engine where no  index is used
    *
    * @param indexUri
    * @return index engine
    */
  def newIndex(indexUri: String): IndexServer = {
    if (null == indexUri || indexUri.isEmpty || !indexUri.contains("://") || !indexUri.startsWith(
          "tudb"
        )) {
      new EmptyIndexServerImpl(Map.empty)
    } else {
      val url = new URI(indexUri)
      val params =
        if (url.getQuery == null) Map.empty[String, String]
        else
          url.getQuery
            .split("&")
            .map(v => v.split("=").toList)
            .map(v => v(0) -> (if (v.size > 1) v(1) else ""))
            .toMap
      params.getOrElse("type", "dummy") match {
        case "memory" => new MemoryIndexServerImpl(params)
        case "rocksdb" =>
          val path = params.getOrElse("path", "")
          if (path == null || path.isEmpty) new EmptyIndexServerImpl(params)
          else new RocksIndexServerImpl(params)
        case _ => new EmptyIndexServerImpl(params)
      }
    }
  }

}
