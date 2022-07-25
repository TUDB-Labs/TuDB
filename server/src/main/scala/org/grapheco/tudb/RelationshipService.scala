package org.grapheco.tudb

import io.grpc.stub.StreamObserver
import org.grapheco.tudb.core.{Core, RelationshipServiceGrpc}

class RelationshipService(dbPath: String, indexUri: String)
  extends RelationshipServiceGrpc.RelationshipServiceImplBase {

  override def createRelationship(
      request: Core.RelationshipCreateRequest,
      responseObserver: StreamObserver[Core.RelationshipCreateResponse]
    ): Unit = {
    // TODO: Create object and persist in DB and set status
    val resp: Core.RelationshipCreateResponse = Core.RelationshipCreateResponse
      .newBuilder()
      .setRelationship(request.getRelationship)
      .setStatus(new Core.GenericResponseStatus("successfully created relationship", 0))
      .build()
    responseObserver.onNext(resp)
    responseObserver.onCompleted()
  }

  override def getRelationship(
      request: Core.RelationshipGetRequest,
      responseObserver: StreamObserver[Core.RelationshipGetResponse]
    ): Unit = {
    val resp: Core.RelationshipGetResponse = Core.RelationshipGetResponse
      .newBuilder()
      // TODO: Get object from DB
      //      .setRelationship(request.getName)
      .setStatus(new Core.GenericResponseStatus("successfully get relationship", 0))
      .build()
    responseObserver.onNext(resp)
    responseObserver.onCompleted()
  }

  override def deleteRelationship(
      request: Core.RelationshipDeleteRequest,
      responseObserver: StreamObserver[Core.RelationshipDeleteResponse]
    ): Unit = {
    // TODO: Delete the object from DB
    val resp: Core.RelationshipDeleteResponse = Core.RelationshipDeleteResponse
      .newBuilder()
      .setStatus(new Core.GenericResponseStatus("successfully deleted relationship", 0))
      .build()
    responseObserver.onNext(resp)
    responseObserver.onCompleted()
  }

  override def listRelationships(
      request: Core.RelationshipListRequest,
      responseObserver: StreamObserver[Core.RelationshipListResponse]
    ): Unit = {
    // TODO: Delete the object from DB
    val resp: Core.RelationshipListResponse = Core.RelationshipListResponse
      .newBuilder()
      // TODO: Get objects from DB
      //      .setRelationships()
      .setStatus(new Core.GenericResponseStatus("successfully listed relationships", 0))
      .build()
    responseObserver.onNext(resp)
    responseObserver.onCompleted()
  }
}
