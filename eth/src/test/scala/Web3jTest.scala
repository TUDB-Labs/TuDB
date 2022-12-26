import com.tudb.blockchain.eth.client.{EthJsonObjectParser, EthNodeClientOnline, EthNodeJsonApi}
import org.web3j.abi.TypeDecoder
import org.web3j.abi.datatypes.generated.Uint256
import org.web3j.protocol.Web3j
import org.web3j.protocol.core.DefaultBlockParameter
import org.web3j.protocol.core.methods.response.EthBlock
import org.web3j.protocol.http.HttpService

import java.math.BigInteger
import scala.collection.JavaConverters._

/**
  *@description:
  */
object Web3jTest {
  def main(args: Array[String]): Unit = {
    val client =
      Web3j.build(new HttpService("https://mainnet.infura.io/v3/6afd74985a834045af3d4d8f6344730a"))

//    val latestBlock = client.ethBlockNumber().send().getBlockNumber
//    println(latestBlock)
//    val txs = client
//      .ethGetBlockByNumber(DefaultBlockParameter.valueOf(latestBlock), true)
//      .send()
//      .getBlock
//      .getTransactions
//      .asScala
//      .toSeq
//    txs.foreach(tx => {
//      val relT = tx.get().asInstanceOf[EthBlock.TransactionObject].get()
//      println(
//        s"hash: ${relT.getHash},start: ${relT.getFrom}, end: ${relT.getTo}, value: ${relT.getValue}"
//      )
//    })

    val contractTxHash = "0xdf7a6802ad726c96ccdf3ab347417203a4dfa6bd19dabdcd7244d08a6f75d9f9"
    val tx = client.ethGetTransactionByHash(contractTxHash).send().getTransaction.get()
    val input = tx.getInput
    val methodId = input.substring(0, 10)
    val decodeNumeric =
      classOf[TypeDecoder].getDeclaredMethod("decodeNumeric", classOf[String], classOf[Class[_]])
    decodeNumeric.setAccessible(true)
    val res = methodId match {
      case "0xa9059cbb" => {
        val to = input.substring(10, 74)
        val value = input.substring(74)
        val res =
          decodeNumeric
            .invoke(null, value, classOf[Uint256])
            .asInstanceOf[Uint256]
            .getValue
            .longValue()
        (to, value)
      }
      case "0x23b872dd" => {
        val from = input.substring(10, 74)
        val to = input.substring(74, 138)
        val value = input.substring(138)
        (from, to, value)
      }
    }
    println("")
  }
}
