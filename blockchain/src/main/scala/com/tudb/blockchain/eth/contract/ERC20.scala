package com.tudb.blockchain.eth.contract

/**
  *@description:
  */
sealed trait ERC20Contract extends Proposer {}

case class ERC20Transfer(toAddress: String, money: String) extends ERC20Contract

case class ERC20TransferFrom(fromAddress: String, toAddress: String, money: String)
  extends ERC20Contract

case class NoneERC20() extends ERC20Contract

object ERC20Meta {
  val CONTRACT_ADDRESS = "0xdac17f958d2ee523a2206206994597c13d831ec7"
  val CONTRACT_METHOD_TRANSFER = "0xa9059cbb"
  val CONTRACT_METHOD_TRANSFER_FROM = "0x23b872dd"
}
