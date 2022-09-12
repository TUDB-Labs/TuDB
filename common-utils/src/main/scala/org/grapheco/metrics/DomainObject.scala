package org.grapheco.metrics

object DomainObject {
  val domainID = "operator"

  var domain: Domain = new Domain(domainID)

  def addRecord(r: Record): Unit = {
    domain.addRecord(r)
  }

  def recordLatency(r: Record): Unit = {
    domain.recordLatency(r)
  }
}
