import com.tudb.blockchain.server.TuDBServerContext
import org.junit.{Assert, Test}

import java.io.{File, FileInputStream}
import java.util.Properties

/**
  *@description:
  */
class ContextTest {

  @Test
  def testContext(): Unit = {
    val conf = "conf/test.conf"
    val properties = new Properties()
    properties.load(new FileInputStream(new File(conf)))
    val context = new TuDBServerContext()
    context.setTuDBPath(properties.getProperty("db-path"))
    context.setTuDBPort(properties.getProperty("tudb-port"))
    context.setEthNodeUrl(properties.getProperty("eth-node-url"))

    Assert.assertEquals("./testdata/test", context.getTuDBPath())
    Assert.assertEquals(9967, context.getTuDBPort())
    Assert.assertEquals(
      "https://mainnet.infura.io/v3/6afd74985a834045af3d4d8f6344730a",
      context.getEthNodeUrl()
    )

  }
}
