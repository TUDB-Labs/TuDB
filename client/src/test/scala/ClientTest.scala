import com.tudb.blockchain.client.TuDBClient

/**
  *@description:
  */
object ClientTest {
  def main(args: Array[String]): Unit = {
    val client = new TuDBClient("127.0.0.1", 9967)
    val res = client.hopQuery("0x12345", "out", 1, 1, 10)
    while (res.hasNext) {
      val data = res.next()
      println(data.getMessage)
      println(data.getResult)
    }
    client.close()
  }
}
