import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import java.math.BigInteger

/**
  *@description:
  */
object TestMethod {
  def main(args: Array[String]): Unit = {
    implicit val formats = DefaultFormats
    val response = TestResponse("eth", "0x111", "0x222", new BigInteger("233", 10).toString(10))
    val json = Serialization.write(Seq(response, response, response))
    println(json)
  }
}

case class TestResponse(token: String, from: String, to: String, money: String)
