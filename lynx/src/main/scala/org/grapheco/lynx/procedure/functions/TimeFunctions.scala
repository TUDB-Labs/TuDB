package org.grapheco.lynx.procedure.functions

import org.grapheco.lynx.func.LynxProcedure
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxMap
import org.grapheco.lynx.types.property.{LynxInteger, LynxNumber, LynxString}
import org.grapheco.lynx.types.time._
import org.grapheco.lynx.util._

class TimeFunctions {
  @LynxProcedure(name = "lynx")
  def lynx(args: Seq[LynxValue]): String = {
    "lynx-0.3"
  }

  @LynxProcedure(name = "power")
  def power(args: Seq[LynxInteger]): Int = {
    val x = args.head
    val n = args.last
    math.pow(x.value, n.value).toInt
  }

  @LynxProcedure(name = "date")
  def date(args: Seq[LynxValue]): LynxDate = {
    args.size match {
      case 0 => LynxDateUtil.now()
      case 1 => LynxDateUtil.parse(args.head).asInstanceOf[LynxDate]
    }
  }

  @LynxProcedure(name = "datetime")
  def datetime(args: Seq[LynxValue]): LynxDateTime = {
    args.size match {
      case 0 => LynxDateTimeUtil.now()
      case 1 => LynxDateTimeUtil.parse(args.head).asInstanceOf[LynxDateTime]
    }
  }

  /**
   * date function [now()]
   * @param args nothing need
   * @return LynxDateTime-Now
   */
  @LynxProcedure(name = "now")
  def now(args: Seq[LynxValue]): LynxDateTime = {
    LynxDateTimeUtil.now()
  }

  @LynxProcedure(name = "localdatetime")
  def localDatetime(args: Seq[LynxValue]): LynxLocalDateTime = {
    args.size match {
      case 0 => LynxLocalDateTimeUtil.now()
      case 1 => LynxLocalDateTimeUtil.parse(args.head).asInstanceOf[LynxLocalDateTime]
    }
  }

  @LynxProcedure(name = "time")
  def time(args: Seq[LynxValue]): LynxTime = {
    args.size match {
      case 0 => LynxTimeUtil.now()
      case 1 => LynxTimeUtil.parse(args.head).asInstanceOf[LynxTime]
    }
  }

  @LynxProcedure(name = "localtime")
  def localTime(args: Seq[LynxValue]): LynxLocalTime = {
    args.size match {
      case 0 => LynxLocalTimeUtil.now()
      case 1 => LynxLocalTimeUtil.parse(args.head).asInstanceOf[LynxLocalTime]
    }
  }

  @LynxProcedure(name = "duration")
  def duration(args: Seq[LynxValue]): LynxDuration = {
    args.head match {
      case LynxString(v) => LynxDurationUtil.parse(v)
      case LynxMap(v) =>
        LynxDurationUtil.parse(
          v.asInstanceOf[Map[String, LynxNumber]].mapValues(_.number.doubleValue())
        )
    }
  }

}
