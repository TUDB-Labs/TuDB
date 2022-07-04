/** Copyright (c) 2022 TuDB * */
package org.grapheco.tudb

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import org.grapheco.lynx.types.composite.{LynxList, LynxMap}
import org.grapheco.lynx.{LynxResult, PathTriple}
import org.grapheco.lynx.types.property.{LynxNumber, LynxPath, LynxString}
import org.grapheco.lynx.types.structural.LynxNodeLabel
import org.grapheco.lynx.types.time.LynxTemporalValue
import org.grapheco.tudb.store.node.TuNode
import org.grapheco.tudb.store.relationship.TuRelationship

import java.text.SimpleDateFormat

/** add toJson method to AnyRef
 *
 * use case:
 *
 * import org.grapheco.tudb.TuDBJsonTool.AnyRefAddMethod
 *
 * val mapString=Map("a"->1,"b"->2).toJson()
 * val nodeString=Node("a").toJson()
 * AnyObject.toJson()
 */
object TuDBJsonTool {
  val objectMapper = new ObjectMapper()
    .findAndRegisterModules()
    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
    // timestamp convert to date
    .configure(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS, false)
    //ignore error
    .configure(SerializationFeature.FAIL_ON_UNWRAPPED_TYPE_IDENTIFIERS, false)
    // enum use  `toString` format
    .configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true)
    // set java.util.Date format
    .setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"))

  implicit class AnyRefAddMethod[A <: AnyRef](bean: A) {

    def toJson(): String = TuDBJsonTool.toJson(bean)

  }

  def toJson(bean: Any): String = {
    bean match {
      case node: TuNode => getJson(node)
      case relationship: TuRelationship => getJson(relationship)
      case subPath: PathTriple => getJson(subPath)
      case path: LynxPath => getJson(path)
      case result: LynxResult => getJson(result)
      case str: LynxString => '"' + str.value + '"'
      case str: String => '"' + str + '"'
      case number: LynxNumber => number.value.toString
      case number: Number => number.toString
      case label: LynxNodeLabel => '"' + label.value + '"'
      case time: LynxTemporalValue => '"' + time.value.toString + '"'
      case list: LynxList => toJson(list.value)
      case map: LynxMap => toJson(map.value)
      case seq: Seq[Any] => "[" + seq.map(toJson).mkString(",") + "]"
      case m: Map[Any, Any] =>
        "{" + m.map(kv => (toJson(kv._1) + ":" + toJson(kv._2))).mkString(",") + "}"
      case v: Any => objectMapper.writeValueAsString(v)
    }
  }

  def getJson(node: TuNode): String = {
    """{"identity":""" + node.id.value + ""","labels":""" + objectMapper.writeValueAsString(
      node.labels.map(_.value)
    ) + ""","properties":""" + objectMapper.writeValueAsString(
      node.properties.map(kv => kv._1 -> kv._2.value)
    ) + """}"""
  }

  def getJson(relationship: TuRelationship): String = {
    """{"identity":""" + relationship.id.value + ""","start":""" + relationship.startId + ""","end":""" +
      relationship.endId + ""","type":""" + toJson(relationship.relationType.get.value) +
      ""","properties":""" + objectMapper.writeValueAsString(
      relationship.properties.map(kv => kv._1 -> kv._2.value)
    ) + """}"""
  }
  def getJson(subPath: PathTriple): String = {
    """{"start":""" + getJson(subPath.startNode.asInstanceOf[TuNode]) + ""","end":""" +
      getJson(subPath.endNode.asInstanceOf[TuNode]) + ""","relationship":""" + getJson(
      subPath.storedRelation.asInstanceOf[TuRelationship]
    ) + """}"""
  }

  def getJson(path: LynxPath): String = {
    """{"start":""" + getJson(path.startNode().asInstanceOf[TuNode]) + ""","end":""" + getJson(
      path.endNode().asInstanceOf[TuNode]
    ) +
      ""","segments":[""" + path.path.map(v => getJson(v)).mkString(",") +
      """],"length":""" + path.path.length + """}"""
  }
  def getJson(result: LynxResult): String = {
    "[" + result
      .records()
      .map { record =>
        "[" + record
          .map { kv =>
            f"""{"keys": [${toJson(kv._1)}],"length": 1,"_fields":[${toJson(kv._2)}]}"""
          }
          .mkString(",") + "]"
      }
      .mkString(",") + "]"
  }

}
