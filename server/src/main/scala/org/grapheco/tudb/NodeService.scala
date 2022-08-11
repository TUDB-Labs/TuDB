package org.grapheco.tudb

import io.grpc.stub.StreamObserver
import org.grapheco.tudb.NodeService.{ConvertToGrpcNode, ConvertToStoredNode}
import org.grapheco.tudb.core.{Core, NodeServiceGrpc}
import org.grapheco.tudb.serializer.NodeSerializer
import org.grapheco.tudb.store.node.{NodeStoreAPI, StoredNodeWithProperty}

// TODO: All messages and exit codes are hard-coded to be successful since NodeStoreAPI does not provide
//   a way to access the statuses.
class NodeService(dbPath: String, indexUri: String, nodeStoreAPI: NodeStoreAPI)
  extends NodeServiceGrpc.NodeServiceImplBase {

  override def createNode(
      request: Core.NodeCreateRequest,
      responseObserver: StreamObserver[Core.NodeCreateResponse]
    ): Unit = {
    val status = Core.GenericResponseStatus
      .newBuilder()
      .setMessage("successfully created node")
      .setExitCode(0)
      .build()
    nodeStoreAPI.addNode(ConvertToStoredNode(request.getNode))
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
      .setNode(ConvertToGrpcNode(nodeStoreAPI.getNodeById(request.getNodeId).get))
      .setStatus(status)
      .build()
    responseObserver.onNext(resp)
    responseObserver.onCompleted()
  }

  override def deleteNode(
      request: Core.NodeDeleteRequest,
      responseObserver: StreamObserver[Core.NodeDeleteResponse]
    ): Unit = {
    nodeStoreAPI.deleteNode(request.getNodeId)
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
    // TODO: Debug why this is 0.
    println(nodeStoreAPI.allNodes().length)
    val nodes = nodeStoreAPI
      .allNodes()
      .map(rawNode => {
        ConvertToGrpcNode(rawNode)
      })
    val status = Core.GenericResponseStatus
      .newBuilder()
      .setMessage("successfully listed nodes")
      .setExitCode(0)
      .build()
    val respBuilder: Core.NodeListResponse.Builder = Core.NodeListResponse.newBuilder()
    nodes.foreach(node => {
      respBuilder.addNodes(node)
    })
    val resp = respBuilder.setStatus(status).build()
    responseObserver.onNext(resp)
    responseObserver.onCompleted()
  }
}

object NodeService {
  def ConvertToGrpcNode(rawNode: StoredNodeWithProperty): Core.Node = {
    val nodeBuilder: Core.Node.Builder = Core.Node
      .newBuilder()
      .setNodeId(rawNode.id)

    rawNode.properties
      .foreach(kv => {
        val prop = Core.Property
          .newBuilder()
          .setInd(kv._1)
          .setValue(kv._2.toString)
          .build()
        nodeBuilder.addProperties(prop)
      })
    // .setLabels(0, rawNode.properties(0).toString)
    nodeBuilder.build()
  }

  def ConvertToStoredNode(node: Core.Node): StoredNodeWithProperty = {
    // TODO: Support labelIds
    val labelIds = Array(1, 2)
    var rawProps = Map[Int, String]()
    node.getPropertiesList.forEach(prop => {
      rawProps += (prop.getInd -> prop.getValue)
    })
    val nodeInBytes: Array[Byte] =
      NodeSerializer.encodeNodeWithProperties(node.getNodeId, labelIds, rawProps)
    new StoredNodeWithProperty(1L, labelIds, nodeInBytes)
  }
}
