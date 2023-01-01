package com.tudb.blockchain.server

import com.tudb.blockchain.BlockchainQueryApi
import com.tudb.blockchain.entities.ResponseTransaction
import com.tudb.blockchain.network.Query.QueryResponse
import com.tudb.blockchain.network.{Query, TuQueryServiceGrpc}
import com.tudb.storage.meta.MetaStoreApi
import io.grpc.stub.StreamObserver
import org.rocksdb.RocksDB

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import java.math.BigInteger
import scala.collection.mutable.ArrayBuffer

/**
  *@description:
  */
class TuDBQueryService(chainDBs: Map[String, RocksDB], metaStoreApi: MetaStoreApi)
  extends TuQueryServiceGrpc.TuQueryServiceImplBase {

  implicit val formats = DefaultFormats

  // TODO: abstract for all blockchain
  val queryApis = chainDBs.map(kv => kv._1 -> new BlockchainQueryApi(kv._2, metaStoreApi))

  override def hopQuery(request: Query.HopQueryRequest, responseObserver: StreamObserver[Query.QueryResponse]): Unit = {
    try {
      val chainName = request.getChainName
      val tokenName = request.getTokenName
      val address = request.getAddress
      val direction = request.getDirection
      val lowerHop = request.getLowerHop // TODO
      val upperHop = request.getUpperHop // TODO
      val limit = request.getLimit
      val queryResult: Seq[ResponseTransaction] = {
        tokenName match {
          case null => {
            direction match {
              case "in" =>
                queryApis(chainName).findInTransaction(address).slice(0, limit).toSeq
              case "out" =>
                queryApis(chainName).findOutTransaction(address).slice(0, limit).toSeq
            }
          }
          case _ => {
            direction match {
              case "in" =>
                queryApis(chainName).findInTransaction(address, tokenName).slice(0, limit).toSeq
              case "out" =>
                queryApis(chainName).findInTransaction(address, tokenName).slice(0, limit).toSeq
            }
          }
        }
      }

      val jsonArray: ArrayBuffer[JSONTransaction] = ArrayBuffer()

      direction match {
        case "in" => {
          queryResult.foreach(result => {
            val obj = JSONTransaction(
              result.from,
              result.to,
              result.timestamp,
              result.token,
              new BigInteger(result.hexStringMoney, 16).toString(10)
            )
            jsonArray.append(obj)
          })
        }
        case "out" => {
          queryResult.foreach(result => {
            val obj = JSONTransaction(
              result.from,
              result.to,
              result.timestamp,
              result.token,
              new BigInteger(result.hexStringMoney, 16).toString(10)
            )
            jsonArray.append(obj)
          })
        }
      }
      val jsonResult = Serialization.write(jsonArray)
      val responder = QueryResponse.newBuilder().setMessage("ok").setResult(jsonResult).build()
      responseObserver.onNext(responder)
      responseObserver.onCompleted()
    } catch {
      case e: Exception => {
        responseObserver.onNext(
          QueryResponse
            .newBuilder()
            .setMessage("SERVER ERROR")
            .build()
        )
      }
      responseObserver.onCompleted()
    }
  }
}
