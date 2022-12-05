package com.tudb.blockchain.server

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.JSON
import com.tudb.blockchain.network.Query.QueryResponse
import com.tudb.blockchain.network.{Query, TuQueryServiceGrpc}
import com.tudb.blockchain.storage.{QueryApi}
import io.grpc.stub.StreamObserver

/**
  *@description:
  */
class TuDBQueryService(api: QueryApi) extends TuQueryServiceGrpc.TuQueryServiceImplBase {
  override def hopQuery(
      request: Query.HopQueryRequest,
      responseObserver: StreamObserver[Query.QueryResponse]
    ): Unit = {
    try {
      val address = request.getAddress
      val direction = request.getDirection
      val lowerHop = request.getLowerHop // TODO
      val upperHop = request.getUpperHop // TODO
      val limit = request.getLimit
      val res = {
        direction match {
          case "in"  => api.findInAddress(address).slice(0, limit).toSeq
          case "out" => api.findOutAddress(address).slice(0, limit).toSeq
        }
      }
      val jsonResult = JSON.toJSONString(new JSONAddress(res), SerializerFeature.SortField)
      val responder = QueryResponse.newBuilder().setMessage("ok").setResult(jsonResult).build()
      responseObserver.onNext(responder)
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
