package org.grapheco.lynx.procedure.functions

import org.grapheco.lynx.func.LynxProcedure
import org.grapheco.lynx.procedure.exceptions.LynxProcedureException
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.{LynxList, LynxMap}
import org.grapheco.lynx.types.property.{LynxFloat, LynxInteger, LynxNumber, LynxString}
import org.grapheco.lynx.types.structural.{LynxNode, LynxRelationship}
import org.grapheco.lynx.types.time._
import org.grapheco.lynx.util._

import java.util.regex.Pattern

class TimeFunctions {
  @LynxProcedure(name = "lynx")
  def lynx(): String = {
    "lynx-0.3"
  }

  @LynxProcedure(name = "power")
  def power(x: LynxInteger, n: LynxInteger): Int = {
    math.pow(x.value, n.value).toInt
  }

  @LynxProcedure(name = "date")
  def date(inputs: LynxValue): LynxDate = {
    LynxDateUtil.parse(inputs).asInstanceOf[LynxDate]
  }

  @LynxProcedure(name = "date")
  def date(): LynxDate = {
    LynxDateUtil.now()
  }

  @LynxProcedure(name = "datetime")
  def datetime(inputs: LynxValue): LynxDateTime = {
    LynxDateTimeUtil.parse(inputs).asInstanceOf[LynxDateTime]
  }

  @LynxProcedure(name = "datetime")
  def datetime(): LynxDateTime = {
    LynxDateTimeUtil.now()
  }

  @LynxProcedure(name = "localdatetime")
  def localDatetime(inputs: LynxValue): LynxLocalDateTime = {
    LynxLocalDateTimeUtil.parse(inputs).asInstanceOf[LynxLocalDateTime]
  }

  @LynxProcedure(name = "localdatetime")
  def localDatetime(): LynxLocalDateTime = {
    LynxLocalDateTimeUtil.now()
  }

  @LynxProcedure(name = "time")
  def time(inputs: LynxValue): LynxTime = {
    LynxTimeUtil.parse(inputs).asInstanceOf[LynxTime]
  }

  @LynxProcedure(name = "time")
  def time(): LynxTime = {
    LynxTimeUtil.now()
  }

  @LynxProcedure(name = "localtime")
  def localTime(inputs: LynxValue): LynxLocalTime = {
    LynxLocalTimeUtil.parse(inputs).asInstanceOf[LynxLocalTime]
  }

  @LynxProcedure(name = "localtime")
  def localTime(): LynxLocalTime = {
    LynxLocalTimeUtil.now()
  }

  @LynxProcedure(name = "duration")
  def duration(input: LynxValue): LynxDuration = {
    input match {
      case LynxString(v) => LynxDurationUtil.parse(v)
      case LynxMap(v) =>
        LynxDurationUtil.parse(
          v.asInstanceOf[Map[String, LynxNumber]].mapValues(_.number.doubleValue())
        )
    }
  }

}
