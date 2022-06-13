package org.grapheco.tudb.importer

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 13:36 2021/1/20
  * @Modified By:
  */
object TimeUtil {
  def millsSecond2Time(ms: Long): String = {
    val ss: Int = 1000
    val mi: Int = ss * 60
    val hh: Int = mi * 60
    val dd: Int = hh * 24

    val day: Int = (ms / dd).toInt
    val hour: Int = ((ms - day * dd) / hh).toInt
    val minute: Int = ((ms - day * dd - hour * hh) / mi).toInt
    val second: Int =
      ((ms - day * dd - hour * hh - minute * mi) * 1.0 / ss).toInt;
    val milliSecond: Int = (ms % 1000).toInt;

    if (day > 0)
      s"$day days $hour hours $minute mins $second secs $milliSecond ms"
    else if (hour > 0) s"$hour hours $minute mins $second secs $milliSecond ms"
    else if (minute > 0) s"$minute mins $second secs $milliSecond ms"
    else if (second > 0) s"$second secs $milliSecond ms"
    else s"$milliSecond ms"
  }

}
