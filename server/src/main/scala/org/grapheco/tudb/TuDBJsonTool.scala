/** Copyright (c) 2022 PandaDB * */
package org.grapheco.tudb

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import org.grapheco.lynx.types.structural.LynxNode
import org.grapheco.tudb.graph.{TuNode, TuRelationship}

import java.text.SimpleDateFormat

object TuDBJsonTool {
  val objectMapper = new ObjectMapper()
    .findAndRegisterModules()
    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
    // 持续时间序列化为字符串
    .configure(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS, false)
    // 当出现 Java 类中未知的属性时不报错，而是忽略此 JSON 字段
    .configure(SerializationFeature.FAIL_ON_UNWRAPPED_TYPE_IDENTIFIERS, false)
    // 枚举类型调用 `toString` 方法进行序列化
    .configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true)
    // 设置 java.util.Date 类型序列化格式
    .setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"))
  def getJson(node: TuNode): String = {
    """{"id":""" + node.id + ""","label":""" + objectMapper.writeValueAsString(
      node.labels
    ) + ""","properties":""" + objectMapper.writeValueAsString(node.properties) + """}"""
  }

  def getJson(relationship: TuRelationship): String = {
    """{"identity":""" + relationship.id + ""","start":""" + relationship.startId + ""","end":""" +
      relationship.endId + ""","type":""" + relationship.relationType.get.value +
      ""","properties":""" + objectMapper.writeValueAsString(relationship.properties) + """}"""
  }

}
