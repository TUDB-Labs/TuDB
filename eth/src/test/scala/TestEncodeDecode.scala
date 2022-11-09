import org.demo.eth.tools.EthTools
import org.junit.{Assert, Test}

/**
  *@description:
  */
class TestEncodeDecode {

  @Test
  def testTransaction(): Unit = {
    val transaction =
      "0x8a3da8b2fce7c23b636b94ad0990e4c011541bdb9623f4933029587dcfaf1a5b".toLowerCase()
    val input = EthTools.removeHexStringHeader(transaction)
    val byteOfHexString = EthTools.hexString2ArrayBytes(input)
    val originHexString = "0x" + EthTools.arrayBytes2HexString(byteOfHexString)
    Assert.assertEquals(transaction, originHexString)
  }

  @Test
  def testAddress(): Unit = {
    val address = "0x0ca255ad35Bf3EAa3ecE1043696cC160465e53B6".toLowerCase()
    val input = EthTools.removeHexStringHeader(address)
    val byteOfHexString = EthTools.hexString2ArrayBytes(input)
    val originHexString = "0x" + EthTools.arrayBytes2HexString(byteOfHexString)
    Assert.assertEquals(address, originHexString)
  }
}
