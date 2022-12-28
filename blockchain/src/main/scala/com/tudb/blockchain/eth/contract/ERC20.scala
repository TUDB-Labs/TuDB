package com.tudb.blockchain.eth.contract

import com.tudb.blockchain.TokenNames

/**
  *@description:
  */
sealed trait ERC20Contract extends Proposer {}

case class ERC20Transfer(toAddress: String, money: String) extends ERC20Contract

case class ERC20TransferFrom(fromAddress: String, toAddress: String, money: String)
  extends ERC20Contract

case class NoneERC20() extends ERC20Contract

object ERC20Meta {
  private val USDT_CONTRACT_ADDRESS = "0xdac17f958d2ee523a2206206994597c13d831ec7"
  private val USDC_CONTRACT_ADDRESS = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
  val CONTRACT_METHOD_TRANSFER = "0xa9059cbb"
  val CONTRACT_METHOD_TRANSFER_FROM = "0x23b872dd"

  val ERC20Contracts: Map[String, String] =
    Map(USDT_CONTRACT_ADDRESS -> TokenNames.USDT, USDC_CONTRACT_ADDRESS -> TokenNames.USDC)
}
