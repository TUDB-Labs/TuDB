// Copyright 2022 The TuDB Authors. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.grapheco.tudb

import io.grpc.stub.StreamObserver
import org.grapheco.tudb.core.{Core, GraphServiceGrpc}

class GraphService(dbPath: String, indexUri: String) extends GraphServiceGrpc.GraphServiceImplBase {

  override def createGraph(
      request: Core.GraphCreateRequest,
      responseObserver: StreamObserver[Core.GraphCreateResponse]
    ): Unit = {
    // TODO: Create object and persist in DB and set status
    val status = Core.GenericResponseStatus
      .newBuilder()
      .setMessage("successfully created graph")
      .setExitCode(0)
      .build()
    val resp: Core.GraphCreateResponse = Core.GraphCreateResponse
      .newBuilder()
      .setGraph(request.getGraph)
      .setStatus(status)
      .build()
    responseObserver.onNext(resp)
    responseObserver.onCompleted()
  }

  override def getGraph(
      request: Core.GraphGetRequest,
      responseObserver: StreamObserver[Core.GraphGetResponse]
    ): Unit = {
    val status = Core.GenericResponseStatus
      .newBuilder()
      .setMessage("successfully got graph")
      .setExitCode(0)
      .build()
    val resp: Core.GraphGetResponse = Core.GraphGetResponse
      .newBuilder()
      // TODO: Get object from DB
      //      .setGraph(request.getName)
      .setStatus(status)
      .build()
    responseObserver.onNext(resp)
    responseObserver.onCompleted()
  }

  override def deleteGraph(
      request: Core.GraphDeleteRequest,
      responseObserver: StreamObserver[Core.GraphDeleteResponse]
    ): Unit = {
    val status = Core.GenericResponseStatus
      .newBuilder()
      .setMessage("successfully deleted graph")
      .setExitCode(0)
      .build()
    // TODO: Delete the object from DB
    val resp: Core.GraphDeleteResponse = Core.GraphDeleteResponse
      .newBuilder()
      .setStatus(status)
      .build()
    responseObserver.onNext(resp)
    responseObserver.onCompleted()
  }

  override def listGraphs(
      request: Core.GraphListRequest,
      responseObserver: StreamObserver[Core.GraphListResponse]
    ): Unit = {
    val status = Core.GenericResponseStatus
      .newBuilder()
      .setMessage("successfully listed graphs")
      .setExitCode(0)
      .build()
    // TODO: Delete the object from DB
    val resp: Core.GraphListResponse = Core.GraphListResponse
      .newBuilder()
      // TODO: Get objects from DB
      //      .setGraphs()
      .setStatus(status)
      .build()
    responseObserver.onNext(resp)
    responseObserver.onCompleted()
  }
}
