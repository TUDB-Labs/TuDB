/** Copyright (c) 2022 PandaDB * */
package org.grapheco.tudb.store.index

import com.typesafe.scalalogging.StrictLogging

/** @author: huagnlin
  * @createDate: 2022-06-20 17:19:08
  * @description: this is the index engine create  factory.
  */
object IndexFactory extends StrictLogging {

  /** create index engine by uri
    * index engine url
    * memory://{any}  use hashmap storage index data
    * es://ip:port   use  es (Elasticsearch) storage index data,ip:port is es service address
    *  db://{path}  use rocksdb storage index data ,path is rocksdb data storage location
    *  empty is empty implement ,  use this engine where no  index is used
    * @param indexUri
    * @return index engine
    */
  def newIndex(indexUri: String): IndexServer = {
    if (null == indexUri || indexUri.isEmpty || indexUri == "empty" || !indexUri.contains("://")) {
      new EmptyIndexServerImpl(indexUri)
    } else {
      val List(indexType, indexValue) = indexUri.split(":").toList
      indexType match {
//        case "hashmap" => new MemoryIndexServerImpl(indexValue)
//        case "db"      => new RocksIndexServerImpl(indexValue)
        case _         => new EmptyIndexServerImpl(indexUri)
      }
    }
  }

}
