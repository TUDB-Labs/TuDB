//package com.tudb.blockchain.server
//
//import com.alibaba.fastjson.serializer.SerializerFeature
//import com.alibaba.fastjson.{JSON, JSONArray}
//import com.tudb.blockchain.network.Query.QueryResponse
//import com.tudb.blockchain.network.{Query, TuQueryServiceGrpc}
//import com.tudb.blockchain.storage.QueryApi
//import io.grpc.stub.StreamObserver
//
///**
//  *@description:
//  */
//class TuDBQueryService(api: QueryApi) extends TuQueryServiceGrpc.TuQueryServiceImplBase {
//  val jsonFeature = SerializerFeature.SortField
//
//  override def hopQuery(
//      request: Query.HopQueryRequest,
//      responseObserver: StreamObserver[Query.QueryResponse]
//    ): Unit = {
//    try {
//      val address = request.getAddress
//      val direction = request.getDirection
//      val lowerHop = request.getLowerHop // TODO
//      val upperHop = request.getUpperHop // TODO
//      val limit = request.getLimit
//      val queryResult = {
//        direction match {
//          case "in"  => api.findInTransaction(address).slice(0, limit).toSeq
//          case "out" => api.findOutTransaction(address).slice(0, limit).toSeq
//        }
//      }
//
//      val jsonArray = new JSONArray()
//      direction match {
//        case "in" => {
//          queryResult.foreach(addressAndWei => {
//            val obj = new JSONTransaction(addressAndWei._1, address, addressAndWei._2)
//            jsonArray.add(obj)
//          })
//        }
//        case "out" => {
//          queryResult.foreach(addressAndWei => {
//            val obj = new JSONTransaction(address, addressAndWei._1, addressAndWei._2)
//            jsonArray.add(obj)
//          })
//        }
//      }
//      val jsonResult = JSON.toJSONString(jsonArray, jsonFeature)
//      val responder = QueryResponse.newBuilder().setMessage("ok").setResult(jsonResult).build()
//      responseObserver.onNext(responder)
//      responseObserver.onCompleted()
//    } catch {
//      case e: Exception => {
//        responseObserver.onNext(
//          QueryResponse
//            .newBuilder()
//            .setMessage("SERVER ERROR")
//            .build()
//        )
//      }
//      responseObserver.onCompleted()
//    }
//  }
//}
