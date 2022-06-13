package org.grapheco.tudb

import scala.collection.Set
import scala.collection.mutable.{Map => MMap}

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 17:50 2022/4/12
  * @Modified By:
  */

class ContextMap {
  private val _map = MMap[String, Any]();

  def keys: Set[String] = _map.keySet;

  protected def put[T](key: String, value: T): T = {
    _map(key) = value
    value
  }

  protected def put[T](value: T)(implicit manifest: Manifest[T]): T =
    put[T](manifest.runtimeClass.getName, value)

  protected def get[T](key: String): T = {
    try {
      _map(key).asInstanceOf[T]
    } catch {
      case e: Exception => throw new Exception(s"Error to get ${key}")
    }
  }

  protected def getOption[T](key: String): Option[T] =
    _map.get(key).map(_.asInstanceOf[T]);

  protected def get[T]()(implicit manifest: Manifest[T]): T = get(
    manifest.runtimeClass.getName
  );

  protected def getOption[T]()(implicit manifest: Manifest[T]): Option[T] =
    getOption(manifest.runtimeClass.getName);

}
