package com.tudb.blockchain.eth

import com.tudb.blockchain.eth.contract.{ERC20Contract, ERC20Meta, ERC20Transfer, ERC20TransferFrom, EthTransaction, NoneERC20}
import org.web3j.abi.TypeDecoder
import org.web3j.abi.datatypes.Address
import org.web3j.abi.datatypes.generated.Uint256
import org.web3j.protocol.core.methods.response.EthBlock

import scala.collection.JavaConverters._

/**
  *@description:
  */
class EthBlockParser() {
  val addressDecoder = classOf[TypeDecoder].getDeclaredMethod("decodeAddress", classOf[String])
  private val numericDecoder =
    classOf[TypeDecoder].getDeclaredMethod("decodeNumeric", classOf[String], classOf[Class[_]])
  addressDecoder.setAccessible(true)
  numericDecoder.setAccessible(true)

  def getBlockTransactions(ethBlock: EthBlock.Block): Seq[EthTransaction] = {
    val transactionObjects =
      ethBlock.getTransactions.asScala.map(tx => tx.get().asInstanceOf[EthBlock.TransactionObject])
    extractBlockTransactions(transactionObjects)
  }

  private def extractBlockTransactions(
      txs: Seq[EthBlock.TransactionObject]
    ): Seq[EthTransaction] = {
    val blockTransactions = txs
      .map(tx => {
        val fromAddress = tx.getFrom
        val toAddress = tx.getTo
        if (toAddress != ERC20Meta.CONTRACT_ADDRESS) {
          val money = tx.getValue.toString(16)
          EthTransaction(fromAddress, toAddress, money)
        } else {
          val input = tx.getInput
          val erc20Contract = parseERC20Contract(input)
          erc20Contract match {
            case ERC20Transfer(to, money) => EthTransaction(fromAddress, to, money)

            case ERC20TransferFrom(from, to, money) => EthTransaction(from, to, money)

            case NoneERC20() => null
          }
        }
      })
      .filter(tx => tx != null)
    blockTransactions
  }

  private def parseERC20Contract(txInput: String): ERC20Contract = {
    val methodId = txInput.substring(0, 10)
    methodId match {
      case ERC20Meta.CONTRACT_METHOD_TRANSFER => {
        val toAddress = decodeAddress(txInput.substring(10, 74))
        val money = decodeMoney(txInput.substring(74))

        ERC20Transfer(toAddress, money)
      }
      case ERC20Meta.CONTRACT_METHOD_TRANSFER_FROM => {
        val fromAddress = decodeAddress(txInput.substring(10, 74))
        val toAddress = decodeAddress(txInput.substring(74, 138))
        val money = decodeMoney(txInput.substring(138))

        ERC20TransferFrom(fromAddress, toAddress, money)
      }
      case _ => NoneERC20()
    }
  }

  private def decodeAddress(address: String): String = {
    addressDecoder.invoke(null, address).asInstanceOf[Address].getValue
  }
  private def decodeMoney(value: String): String = {
    numericDecoder
      .invoke(null, value, classOf[Uint256])
      .asInstanceOf[Uint256]
      .getValue
      .toString(16)
  }
}
