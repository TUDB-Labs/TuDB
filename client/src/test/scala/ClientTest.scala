import com.tudb.blockchain.client.TuDBClient

/**
  *@description:
  */
object ClientTest {

  /*
   *0x58124619a4ea38d215d36c962ac5cd73462acdd6
0x594cb208b5bb48db1bcbc9354d1694998864ec63
0x5954ab967bc958940b7eb73ee84797dc8a2afbb9
0x59728544b08ab483533076417fbbb2fd0b17ce3a
0x5a54fe5234e811466d5366846283323c954310b2
0x5bcbdfb6cc624b959c39a2d16110d1f2d9204f72
   *
   */
  def main(args: Array[String]): Unit = {
    val client = new TuDBClient("127.0.0.1", 9967)
    val res = client.hopQuery("0x5c891d76584b46bc7f1e700169a76569bb77d2db", "in", 1, 1, 10)
    println(res.getResult)
    client.close()
  }
}
