package org.grapheco.tudb.importer

import com.typesafe.scalalogging.LazyLogging
import org.grapheco.lynx.util.LynxDateUtil

import java.io.File
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 9:35 2021/1/15
  * @Modified By:
  */
trait SingleFileImporter extends LazyLogging {
  val csvFile: File
  val idIndex: Int
  val labelIndex: Int
  val importerFileReader: ImporterFileReader
  val headLine: Array[String]
  val estLineCount: Long
  val taskCount: Int
  val cmd: ImportCmd

  // [index, (propId, propTypeId)]
  val propHeadMap: Map[Int, (Int, String)]

  protected def _importTask(taskId: Int): Boolean

  protected def _getPropMap(
      lineArr: Array[String],
      propHeadMap: Map[Int, (Int, String)]
    ): Map[Int, Any] = {
    var propMap: Map[Int, Any] = Map[Int, Any]()
    propHeadMap.foreach(kv => {
      val index = kv._1
      val propId = kv._2._1
      val propValue: Any =
        try {
          kv._2._2 match {
            case "long"    => lineArr(index).toLong
            case "int"     => lineArr(index).toInt
            case "boolean" => lineArr(index).toBoolean
            case "double"  => lineArr(index).toDouble
            case "string"  => lineArr(index).replace("\"", "")
            case "date"    => LynxDateUtil.parse(lineArr(index))
            case "long[]" =>
              lineArr(index).trim
                .replace("[", "")
                .replace("]", "")
                .split(cmd.arrayDelimeter)
                .map(item => item.trim.toLong)
                .toArray[Any]
            case "int[]" =>
              lineArr(index).trim
                .replace("[", "")
                .replace("]", "")
                .split(cmd.arrayDelimeter)
                .map(item => item.trim.toInt)
                .toArray[Any]
            case "string[]" =>
              lineArr(index)
                .replace("[", "")
                .replace("]", "")
                .split(cmd.arrayDelimeter)
                .map(f => f.trim)
                .toArray[Any]
            case "boolean[]" =>
              lineArr(index).trim
                .replace("[", "")
                .replace("]", "")
                .split(cmd.arrayDelimeter)
                .map(item => item.trim.toBoolean)
                .toArray[Any]
            case "double[]" =>
              lineArr(index).trim
                .replace("[", "")
                .replace("]", "")
                .split(cmd.arrayDelimeter)
                .map(item => item.trim.toDouble)
                .toArray[Any]
            case _ => lineArr(index).replace("\"", "")
          }
        } catch {
          case ex: Exception =>
            throw new Exception(
              s"Bad content at ${lineArr.mkString(",")} in ${csvFile.getAbsoluteFile}, error occurs at column $index."
            )
        }
      propMap += (propId -> propValue)
    })
    propMap
  }

  def estLineCount(file: File): Long = {
    CSVIOTools.estLineCount(file)
  }

  def importData(): Unit = {
    val taskId: AtomicInteger = new AtomicInteger(0)
    val taskArray: Array[Future[Boolean]] =
      new Array[Int](taskCount).map(item => Future { _importTask(taskId.getAndIncrement()) })
    taskArray.foreach(task => { Await.result(task, Duration.Inf) })
    _commitInnerFileStatToGlobal()
  }

  protected def _countMapAdd(countMap: mutable.Map[Int, Long], key: Int, addBy: Long): Long = {
    if (countMap.contains(key)) {
      val countBeforeAdd: Long = countMap.getOrElse(key, 0.toLong)
      val countAfterAdd: Long = countBeforeAdd + addBy
      countMap.put(key, countAfterAdd)
      countAfterAdd
    } else {
      countMap.put(key, addBy)
      addBy
    }

  }

  protected def _commitInnerFileStatToGlobal(): Boolean

}
