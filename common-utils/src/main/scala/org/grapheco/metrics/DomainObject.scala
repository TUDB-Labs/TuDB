package org.grapheco.metrics

object DomainObject {
  val domainID = "query"

  var domain: Domain = new Domain(domainID)

  def addRecord(r: Record): Unit = {
    domain.addRecord(r)
  }

  def recordLatency(labels: Set[String]): Unit = {
    val r = new Record(new Label(labels), new Value(0))
    domain.recordLatency(r)
  }
}
