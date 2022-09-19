package org.grapheco.metrics

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit._

class Timestamp {
  val timestamp = LocalDateTime.now()

  override def toString(): String = {
    timestamp.toString
  }

  def -(t: Timestamp): Long = {
    MILLIS.between(timestamp, t.timestamp)
  }
}
