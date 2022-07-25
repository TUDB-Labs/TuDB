package org.grapheco.tudb

import io.grpc.stub.StreamObserver
import org.grapheco.tudb.core.{Core, GraphServiceGrpc}

class GraphService(dbPath: String, indexUri: String) extends GraphServiceGrpc.GraphServiceImplBase {

  override def createGraph(
      request: Core.GraphCreateRequest,
      responseObserver: StreamObserver[Core.GraphCreateResponse]
    ): Unit = {
    // TODO: Create object and persist in DB and set status
    val resp: Core.GraphCreateResponse = Core.GraphCreateResponse
      .newBuilder()
      .setGraph(request.getGraph)
      .setStatus(new Core.GenericResponseStatus("successfully created graph", 0))
      .build()
    responseObserver.onNext(resp)
    responseObserver.onCompleted()
  }

  override def getGraph(
      request: Core.GraphGetRequest,
      responseObserver: StreamObserver[Core.GraphGetResponse]
    ): Unit = {
    val resp: Core.GraphGetResponse = Core.GraphGetResponse
      .newBuilder()
      // TODO: Get object from DB
      //      .setGraph(request.getName)
      .setStatus(new Core.GenericResponseStatus("successfully get graph", 0))
      .build()
    responseObserver.onNext(resp)
    responseObserver.onCompleted()
  }

  override def deleteGraph(
      request: Core.GraphDeleteRequest,
      responseObserver: StreamObserver[Core.GraphDeleteResponse]
    ): Unit = {
    // TODO: Delete the object from DB
    val resp: Core.GraphDeleteResponse = Core.GraphDeleteResponse
      .newBuilder()
      .setStatus(new Core.GenericResponseStatus("successfully deleted graph", 0))
      .build()
    responseObserver.onNext(resp)
    responseObserver.onCompleted()
  }

  override def listGraphs(
      request: Core.GraphListRequest,
      responseObserver: StreamObserver[Core.GraphListResponse]
    ): Unit = {
    // TODO: Delete the object from DB
    val resp: Core.GraphListResponse = Core.GraphListResponse
      .newBuilder()
      // TODO: Get objects from DB
      //      .setGraphs()
      .setStatus(new Core.GenericResponseStatus("successfully listed graphs", 0))
      .build()
    responseObserver.onNext(resp)
    responseObserver.onCompleted()
  }
}
