package com.tudb.blockchain.tools

import org.junit.{Assert, Test}

/**
  *@description:
  */
class DataConverterTest {

  @Test
  def testNegation: Unit = {
    val hexTime = "63895dab"
    val bytes = DataConverter.hexString2Long2Bytes(hexTime)
    val str = DataConverter.bytes2Long2hexString(bytes)
    Assert.assertEquals(hexTime, str)
  }
}
