package org.grapheco.tudb

import io.grpc.stub.StreamObserver
import org.grapheco.tudb.core.{Core, NodeServiceGrpc}

class NodeService(dbPath: String, indexUri: String) extends NodeServiceGrpc.NodeServiceImplBase {

  override def createNode(
      request: Core.NodeCreateRequest,
      responseObserver: StreamObserver[Core.NodeCreateResponse]
    ): Unit = {
    val status = Core.GenericResponseStatus
      .newBuilder()
      .setMessage("successfully created node")
      .setExitCode(0)
      .build()
    // TODO: Create node and persist in DB and set status
    val resp: Core.NodeCreateResponse = Core.NodeCreateResponse
      .newBuilder()
      .setNode(request.getNode)
      .setStatus(status)
      .build()
    responseObserver.onNext(resp)
    responseObserver.onCompleted()
  }

  override def getNode(
      request: Core.NodeGetRequest,
      responseObserver: StreamObserver[Core.NodeGetResponse]
    ): Unit = {
    val status = Core.GenericResponseStatus
      .newBuilder()
      .setMessage("successfully got node")
      .setExitCode(0)
      .build()
    val resp: Core.NodeGetResponse = Core.NodeGetResponse
      .newBuilder()
      // TODO: Get node from DB
      //      .setNode(request.getName)
      .setStatus(status)
      .build()
    responseObserver.onNext(resp)
    responseObserver.onCompleted()
  }

  override def deleteNode(
      request: Core.NodeDeleteRequest,
      responseObserver: StreamObserver[Core.NodeDeleteResponse]
    ): Unit = {
    val status = Core.GenericResponseStatus
      .newBuilder()
      .setMessage("successfully deleted node")
      .setExitCode(0)
      .build()
    // TODO: Delete the node from DB
    val resp: Core.NodeDeleteResponse = Core.NodeDeleteResponse
      .newBuilder()
      .setStatus(status)
      .build()
    responseObserver.onNext(resp)
    responseObserver.onCompleted()
  }

  override def listNodes(
      request: Core.NodeListRequest,
      responseObserver: StreamObserver[Core.NodeListResponse]
    ): Unit = {
    val status = Core.GenericResponseStatus
      .newBuilder()
      .setMessage("successfully listed nodes")
      .setExitCode(0)
      .build()
    // TODO: Delete the node from DB
    val resp: Core.NodeListResponse = Core.NodeListResponse
      .newBuilder()
      // TODO: Get nodes from DB
      //      .setNodes()
      .setStatus(status)
      .build()
    responseObserver.onNext(resp)
    responseObserver.onCompleted()
  }
}
