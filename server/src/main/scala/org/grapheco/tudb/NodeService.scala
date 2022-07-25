package org.grapheco.tudb

import io.grpc.stub.StreamObserver
import org.grapheco.tudb.core.{Core, NodeServiceGrpc}

class NodeService(dbPath: String, indexUri: String) extends NodeServiceGrpc.NodeServiceImplBase {

  override def createNode(
      request: Core.NodeCreateRequest,
      responseObserver: StreamObserver[Core.NodeCreateResponse]
    ): Unit = {
    // TODO: Create node and persist in DB and set status
    val resp: Core.NodeCreateResponse = Core.NodeCreateResponse
      .newBuilder()
      .setNode(request.getNode)
      .setStatus(new Core.GenericResponseStatus("successfully created node", 0))
      .build()
    responseObserver.onNext(resp)
    responseObserver.onCompleted()
  }

  override def getNode(
      request: Core.NodeGetRequest,
      responseObserver: StreamObserver[Core.NodeGetResponse]
    ): Unit = {
    val resp: Core.NodeGetResponse = Core.NodeGetResponse
      .newBuilder()
      // TODO: Get node from DB
      //      .setNode(request.getName)
      .setStatus(new Core.GenericResponseStatus("successfully get node", 0))
      .build()
    responseObserver.onNext(resp)
    responseObserver.onCompleted()
  }

  override def deleteNode(
      request: Core.NodeDeleteRequest,
      responseObserver: StreamObserver[Core.NodeDeleteResponse]
    ): Unit = {
    // TODO: Delete the node from DB
    val resp: Core.NodeDeleteResponse = Core.NodeDeleteResponse
      .newBuilder()
      .setStatus(new Core.GenericResponseStatus("successfully deleted node", 0))
      .build()
    responseObserver.onNext(resp)
    responseObserver.onCompleted()
  }

  override def listNodes(
      request: Core.NodeListRequest,
      responseObserver: StreamObserver[Core.NodeListResponse]
    ): Unit = {
    // TODO: Delete the node from DB
    val resp: Core.NodeListResponse = Core.NodeListResponse
      .newBuilder()
      // TODO: Get nodes from DB
      //      .setNodes()
      .setStatus(new Core.GenericResponseStatus("successfully listed nodes", 0))
      .build()
    responseObserver.onNext(resp)
    responseObserver.onCompleted()
  }
}
