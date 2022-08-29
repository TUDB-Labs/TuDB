package org.grapheco.utils.metrics

import java.time.LocalDateTime

class Timestamp {
  val timestamp = LocalDateTime.now()

  def ToString(): String= {
    timestamp.toString
  }
}
