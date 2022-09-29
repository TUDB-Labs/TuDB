package org.grapheco.metrics

import org.junit.{Before, Test}

import scala.Console.println

class DomainObjectTest {
  val labelStr1 = "label1"
  val labelStr2 = "label2"
  val labelStr3 = "label3"

  @Before
  def init(): Unit = {
    DomainObject.clearLabels()
    DomainObject.clearRecords()
  }

  @Test
  def testLabelPushAndPop(): Unit = {
    DomainObject.pushLabel(labelStr1)
    DomainObject.pushLabel(labelStr2)
    assert(DomainObject.stackedLabels.length == 2)

    DomainObject.popLabel()
    DomainObject.pushLabel(labelStr3)
    assert(DomainObject.stackedLabels.length == 2)

    DomainObject.pushLabel(labelStr1)
    assert(DomainObject.stackedLabels.length == 2)
  }

  @Test
  def testAddRecord(): Unit = {
    DomainObject.pushLabel(labelStr1)
    DomainObject.recordLatency(Set(labelStr2))

    assert(DomainObject.domain.records.length == 1)
    var r = DomainObject.domain.records(0)
    assert(r.label.matches(new Label(Set(labelStr1, labelStr2))))

    DomainObject.pushLabel(labelStr2)
    DomainObject.recordLatency(null)
    assert(DomainObject.domain.records.length == 1)
    r = DomainObject.domain.records(0)
    assert(r.label.matches(new Label(Set(labelStr1, labelStr2))))
  }

}
