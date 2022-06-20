package org.grapheco.tudb.store.index

import com.typesafe.scalalogging.StrictLogging

/**
 * build index impl
 */
object IndexFactory extends StrictLogging {

  def newIndex(indexUri: String): IndexServer = {
    if (null == indexUri || indexUri.isEmpty || indexUri == "none" || !indexUri.contains("://")) {
      new EmptyIndexServerImpl(indexUri)
    } else {
      val List(indexType, indexValue) = indexUri.split(":").toList
      indexType match {
        case "hashmap" => new MemoryIndexServerImpl(indexValue)
        case "db"=> new RocksIndexServerImpl(indexValue)
        case _ => new EmptyIndexServerImpl(indexUri)
      }
    }
  }


}
