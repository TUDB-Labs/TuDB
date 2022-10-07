# Protocol, Services, and Interfaces Design

## Protocol Design

### Object Definitions

Below are the main objects in the protocols. Our services are built on these objects.

```
// Element is the basic structure for both Node and Relationship. It's ID must be unique to its associated Node or Relationship.
// An element also maintains a collection of Properties. An element must belong to a graph.
message Element {
  int32 id = 1;
  string name = 2;
  // The graph ID associated with this element.
  int32 graph_id = 3;
  repeated Property properties = 4;
}

// Property holds a key-value pair that's associated with an element.
message Property {
  // The element ID associated with this property.
  int32 element_id = 1;
  string key = 2;
  google.protobuf.Any value = 3;
}

// Graph is a collection of nodes and relationships.
message Graph {
  int32 id = 1;
  string name = 2;
  repeated Node nodes = 3;
  repeated Relationship relationships = 4;
}

// Node extends Element and connects to relationships.
message Node {
  // The following fields are inherited from Element but we write them here explictly since proto3 does not support inheritance.
  int32 id = 1
  string name = 2;
  int32 graph_id = 3; // The graph ID associated with this node.
  repeated Property properties = 4;

  repeated string labels = 5;
  Relationship in_relation = 6;
  Relationship out_relation = 7;
}

// Relationship extends Element and connects two nodes.
message Relationship {
  // The following fields are inherited from Element but we write them here explictly since proto3 does not support inheritance.
  string id = 1
  string name = 2;
  int32 graph_id = 3; // The graph ID associated with this relationship.
  repeated Property properties = 4;
  
  Node start_node = 5;
  Node end_node = 6;
  // The type of relationship between the two connected nodes.
  string relationType = 7;
}
```

### Generic Query

We support generic query for advanced users to send query directly to the database:

The request only contains statement:

```
message GenericQueryRequest {
  string statement = 1;
}
```


The response contains results (e.g. graph objects), message, and exit code:
```
message GenericResponseStatus {
  string message = 2;
  int32 exit_code = 3;
}

message GenericQueryResponse {
  repeated map<string, google.protobuf.Any> results = 1;
  GenericResponseStatus status = 2;
}
```

The service definition is as follows:
```
service QueryService {
  rpc Query(GenericQueryRequest) returns (stream GenericQueryResponse) {}
}
```

### Object-specific Services

We support object-specific services such as create/get/delete graphs to provide easy-to-use endpoints to our REST client
and high-level SDKs.

Let's use graph service as an example:

```
message GraphCreateRequest {
  Graph graph = 1;
};

message GraphCreateResponse {
  Graph graph = 1;
  GenericResponseStatus status = 2;
};

message GraphGetRequest {
  string name = 1;
};

message GraphGetResponse {
  Graph graph = 1;
  GenericResponseStatus status = 2;
};

message GraphListRequest {
  // TODO: Some filtering options
};

message GraphListResponse {
  repeated Graph graphs = 1;
  GenericResponseStatus status = 2;
};

message GraphDeleteRequest {
  string name = 1;
};

message GraphDeleteResponse {
  GenericResponseStatus status = 1;
};

service GraphService {
  rpc CreateGraph(GraphCreateRequest) returns GraphCreateResponse {
    option (google.api.http) = {
      post : "/api/v1/graphs/{graph_name}"
      body : "*"
    };
  }
  
  rpc GetGraph(GraphGetRequest) returns GraphGetResponse {
    option (google.api.http).get = "/api/v1/graphs/{name}";
  }
  
  rpc ListGraphs(GraphListRequest) returns GraphListResponse {
    option (google.api.http).get = "/api/v1/graphs";
  }
  
  rpc DeleteGraph(GraphDeleteRequest) returns GraphDeleteResponse {
    option (google.api.http).get = "/api/v1/graphs/{name}";
  }
}
```

Services for other objects are similar.

## Interfaces

### REST APIs

We provide three types of methods:

1. GET: Obtain data from the database;
2. POST: Send/modify data to the database; 
3. DELETE: Delete data from the database.

These three methods are available for the all the core objects in the graph database.

**Note that all following endpoints will be prefixed by `/api/v1` so that this document is more concise.**

For example, the following endpoints are available for graphs:

1. `GET /graphs`: obtain a list of graphs;
2. `GET /graphs/<graph_name>`: obtain a particular graph;
3. `POST /graphs/<graph_name>`: create a new graph in the database;
4. `DELETE /graphs/<graph_name>`: delete an existing graph.

The following endpoints are available for nodes:
1. `GET /graphs/<graph_name>/nodes`: obtain a list of nodes in a particular graph;
2. `GET /graphs/<graph_name>/nodes/labels`: obtain a list of nodes in a particular graph by labels;
3. `GET /graphs/<graph_name>/nodes/<node_name>`: get a particular node from a graph;
4. `POST /graphs/<graph_name>/nodes/<node_name>`: create a new node in the graph;
5. `DELETE /graphs/<graph_name>/nodes/<node_name>`: delete an existing node;
6. `DELETE /graphs/<graph_name>/nodes`: delete all nodes in the graph;
7. `DELETE /graphs/<graph_name>/nodes/labels`: delete all nodes in the graph by labels.

The endpoints are available for relationships are similar.

Each endpoint also allows specifying query parameters, e.g. `GET /graphs/<graph_name>/?<query_params>`.

The returned object looks like the following:

```
{
  "version": {
    "api": "v2",
    "schema": 0
  },
  "exitCode": 0,
  "message": "successfully retrieved",
  "results": [
    {
      "id": "0",
      "nodes": [...]
    },
    ...
  ]
}
```

#### OpenAPI Spec

REST APIs can be generated via [grpc-gateway](https://github.com/grpc-ecosystem/grpc-gateway). We can [automatically generate OpenAPI spec](https://bbengfort.github.io/2021/01/grpc-openapi-docs/) as well.

### SDKs

We also provide SDKs such as Python SDK to our users.

First, initialize the client:

```python
client = tudb.Client(address=xxx, config=xxx)
```

We can then get the graphs and nodes similar to REST APIs:

```python
// Get the list of graphs
client.list_graphs()
// Get a particular graph
graph = client.get_graph(name=xxx)
// Create a new graph
graph = client.create_graph(Graph(...))
// Delete a graph
client.delete_graph(name=xxx)

// Get the list of the nodes associated with the graph
graph.list_nodes(labels=xxx)
// Get a particular node
graph.get_node(name=xxx)
// Create a node
graph.create_node(Node(...))
// Delete a node
graph.delete_node(name=xxx)

// Get the list of relationships associated with the graph
graph.list_relationships()
// Get a particular relationship
graph.get_relationship(name=xxx)
// Create a relationship
graph.create_relationship(Relationship(...))
// Delete a relationship
graph.delete_relationship(name=xxx)
```

### CLI

Our CLI provides the following commands where `object_type` can be one of the following: graph, node, and relationship.

1. `tudb list <object_type>`: get a list of objects of this type.
2. `tudb get <object_type> <object_name>`: get a single object of this type.
3. `tudb create <object_type> -f <path_to_graph_definition>`: create an object of this type based on the specified definition.
4. `tudb delete <object_type> <object_name>`: delete a single object of this type.
5. `tudb delete <object_type> --all`: delete all objects of this type.

For nodes, there are also additional flags:
1. `--labels`: retrieving nodes that have this list of labels.

For both nodes and relationships, we also need to provide `--graph <graph_name>` to associate with a graph when running the queries.


## Implementation Plan

Some decisions after discussions:

1. The current protobuf definition for core objects are unnecessarily complex and designed to support Gremlin in the future. However,
   we'd like to simplify the implementation of the service so that those definitions (e.g. Node and Relationship) are as close to
   the current storage API as possible. For example, Element and Property are unnecessary and can be removed for the initial implementation.
   We can revisit them later once we re-evaluate whether/how to support Gremlin.
2. `GraphService`, `NodeService`, and `RelationshipService` focus on providing simple get/create/list methods that can be used for REST/CLI/Python clients.
   They will be implemented based on storage APIs. However, if users want to perform complex queries, the existing `TuDBServer` will be leveraged instead.

The implementation plan for the Scala gRPC services consists of the following tasks:

1. Add get/create/list methods in `GraphService`, `NodeService`, and `RelationshipService` that implement `GraphServiceGrpc.GraphServiceImplBase` (without integration with storage yet).
2. Implement `NodeService` that uses `NodeStoreAPI`. For each node, we will include a `graph_id` in `StoredNodeWithProperty`'s properties so that later
   we can use it to look up nodes that belong to a graph in `GraphService`.
3. Implement `RelationshipService` that uses `RelationshipStoreAPI`. Similarly, we will include a reference to the graph it belongs to in `StoredRelationshipWithProperty`'s properties.
4. Implement `GraphService` that constructs a graph object with the list of nodes and relationships. Since the graph can get extremely large, we will support parameters
   like `nodeCount` and `relationshipCount` similar to Neo4j's [`graph.list()`](https://neo4j.com/docs/graph-data-science/current/graph-list/) method to allow getting a subset of the nodes
   and relationships.

The next phase of the development will be building the RESTful, CLI and Python interfaces based on the gRPC services implemented above.
1. RESTful APIs can be generated via [grpc-gateway](https://github.com/grpc-ecosystem/grpc-gateway). We can [automatically generate OpenAPI spec](https://bbengfort.github.io/2021/01/grpc-openapi-docs/) as well.
2. CLI will be built in Scala that invokes the gRPC services.
3. Python interface will be implemented in Python via gRPC calls to the running Scala gRPC services.

## References

* [Gremlin structure APIs](https://github.com/apache/tinkerpop/tree/master/gremlin-core/src/main/java/org/apache/tinkerpop/gremlin/structure)
* [Neo4j Gremlin structure APIs](https://github.com/apache/tinkerpop/tree/master/neo4j-gremlin/src/main/java/org/apache/tinkerpop/gremlin/neo4j/structure)
* [TigerGraph REST APIs](https://docs.tigergraph.com.cn/dev/restpp-api/introduction)
