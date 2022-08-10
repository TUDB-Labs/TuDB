package org.grapheco.tudb

import io.grpc.stub.StreamObserver
import org.grapheco.tudb.core.{Core, NodeServiceGrpc}
import org.grapheco.tudb.serializer.NodeSerializer
import org.grapheco.tudb.store.node.{NodeStoreAPI, StoredNodeWithProperty}

class NodeService(dbPath: String, indexUri: String, nodeStoreAPI: NodeStoreAPI)
  extends NodeServiceGrpc.NodeServiceImplBase {

  def convertToGrpcNode(rawNode: StoredNodeWithProperty): Core.Node = {
    Core.Node
      .newBuilder()
      .setId(1)
      //      .addAllLabels(rawNode.labelIds.toIterable)
      //    rawNode.properties.values.foreach(prop => {
      //      Core.Property.newBuilder().setKey(prop.).setValue().build()
      //      node.setProperties()
      //    })
      // .setLabels(0, rawNode.properties(0).toString)
      .build()
  }

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
    val labelIds1: Array[Int] = Array(1, 2)
    val props1: Map[Int, Any] =
      Map(1 -> 1L, 2 -> "bluejoe", 3 -> 1979.12, 4 -> "cnic")
    val node1InBytes: Array[Byte] =
      NodeSerializer.encodeNodeWithProperties(1L, labelIds1, props1)
    val storedNode = new StoredNodeWithProperty(1L, labelIds1, node1InBytes)
    nodeStoreAPI.addNode(storedNode)
    val resp: Core.NodeCreateResponse = Core.NodeCreateResponse
      .newBuilder()
      .setNode(convertToGrpcNode(storedNode))
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
      .setNode(convertToGrpcNode(nodeStoreAPI.getNodeById(1L).get))
      .setStatus(status)
      .build()
    responseObserver.onNext(resp)
    responseObserver.onCompleted()
  }

  override def deleteNode(
      request: Core.NodeDeleteRequest,
      responseObserver: StreamObserver[Core.NodeDeleteResponse]
    ): Unit = {
//    nodeStoreAPI.deleteNode(request.getNodeId)
    val status = Core.GenericResponseStatus
      .newBuilder()
      .setMessage("successfully deleted node")
      .setExitCode(0)
      .build()
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
    val nodes = nodeStoreAPI
      .allNodes()
      .map(rawNode => {
        convertToGrpcNode(rawNode)
      })
    val status = Core.GenericResponseStatus
      .newBuilder()
      .setMessage("successfully listed nodes")
      .setExitCode(0)
      .build()
    val resp: Core.NodeListResponse.Builder = Core.NodeListResponse.newBuilder()
    nodes.foreach(node => {
      resp.addNodes(node)
    })
    responseObserver.onNext(resp.setStatus(status).build())
    responseObserver.onCompleted()
  }
}
