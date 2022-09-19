package org.grapheco.metrics

import org.junit.{Before, Test}

class DomainTest {
  val domainID = "did"

  val str1 = "a"
  val str2 = "b"
  val str3 = "c"

  val set1: Set[String] = Set(str1)
  val set2: Set[String] = Set(str1, str2)
  val set3: Set[String] = Set(str1, str2)
  val set4: Set[String] = Set(str2, str3)
  val set5: Set[String] = Set(str3)

  val l1 = new Label(set1)
  val l2 = new Label(set2)
  val l3 = new Label(set3)
  val l4 = new Label(set4)
  val l5 = new Label(set5)

  val v1 = new Value(1)
  val v2 = new Value(2)
  val v3 = new Value(3)
  val v4 = new Value(4)
  val v5 = new Value(5)

  val r1 = new Record(l1, v1)
  val r2 = new Record(l2, v2)
  val r3 = new Record(l3, v3)
  val r4 = new Record(l4, v4)
  val r5 = new Record(l5, v5)

  var domain = new Domain(domainID)

  @Before
  def init(): Unit = {}

  @Test
  def testLabels(): Unit = {
    domain.addRecord(r1)
    domain.addRecord(r2)
    domain.addRecord(r3)
    domain.addRecord(r4)
    domain.addRecord(r5)

    val filterl1 = new Label(Set(str1))
    val filterl2 = new Label(Set(str2))
    val filterl3 = new Label(Set(str3))

    val records1 = domain.filterRecords(filterl1)
    assert(records1.size == 3)

    val records2 = domain.filterRecords(filterl2)
    assert(records2.size == 3)

    val records3 = domain.filterRecords(filterl3)
    assert(records3.size == 2)

    assert(r2.containLabel(l1))
    assert(r3.matchLabel(r2))
  }

  @Test
  def testRecordLatency(): Unit = {
    domain.recordLatency(r2)
    assert(domain.records.length == 1)

    domain.recordLatency(r3)
    assert(domain.records.length == 1)
  }
}
