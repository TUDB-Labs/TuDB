package org.grapheco.tudb.store.index

import com.typesafe.scalalogging.StrictLogging

/**
 * build index impl
 */
object IndexBuilder extends StrictLogging {

  def newIndex(indexUri: String): IndexSPI = {
    if (null == indexUri || indexUri.isEmpty || indexUri == "none" || !indexUri.contains("://")) {
      new EmptyIndexAPI(indexUri)
    } else {
      val List(indexType, indexValue) = indexUri.split(":").toList
      indexType match {
        case "hashmap" => new MemoryIndexAPI(indexValue)
        case "db"=> new RocksIndexAPI(indexValue)
        case _ => new EmptyIndexAPI(indexUri)
      }
    }
  }


}
