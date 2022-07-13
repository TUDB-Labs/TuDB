# Protocol, Services, and Interfaces Design

## Protocol Design

### Object Definitions

Below are the main objects in the protocols. Our services are built on these objects.

```
message Element {
  string name = 1;
  // The graph associated with this element.
  Graph graph = 3;
}

message Property {
  // The element associated with this property.
  Element element = 1;
  string key = 2;
  google.protobuf.Any value = 3;
}

message Graph {
  string name = 1;
  repeated Node nodes = 2;
  repeated Relationship relationships = 3;
}

message Node {
  string name = 1;
  repeated string labels = 2;
  repeated Property properties = 3;
  Relationship in_relation = 4;
  Relationship out_relation = 5;
}

message Relationship {
  string name = 1;
  Node start_node = 2;
  Node end_node = 3;
  repeated Property properties = 4;
  string relationType = 5;
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
message GenericQueryResponse {
  repeated map<string, google.protobuf.Any> results = 1;
  string message = 2;
  int32 exit_code = 3;
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

message GraphGetRequest {
  string name = 1;
};

message GraphListRequest {
  // TODO: Some filtering options
};

service GraphService {
  rpc CreateGraph(GraphCreateRequest) returns GenericQueryResponse {
    option (google.api.http) = {
      post : "/api/v1/graphs/{graph_name}"
      body : "*"
    };
  }
  
  rpc GetGraph(GraphGetRequest) returns GenericQueryResponse {
    option (google.api.http).get = "/api/v1/graphs/{name}";
  }
  
  rpc ListGraphs(GraphListRequest) returns GenericQueryResponse {
    option (google.api.http).get = "/api/v1/graphs";
  }
  
  rpc DeleteGraph(GraphDeleteRequest) returns GenericQueryResponse {
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

For example, the following endpoints are available for graphs (actual endpoints will be prefixed by `/api/v1`):

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
