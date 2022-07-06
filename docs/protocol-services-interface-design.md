# Protocol, Services, and Interfaces Design

## Protocol Definitions

### Object Definitions

Graph, node, and relationship are the main objects in the protocols. Our services are built on these objects.

```
message Graph {
  string name = 1;
  repeated Node nodes = 2;
  repeated Relationship relationships = 3;
}

message Node {
  string name = 1;
  repeated string labels = 2;
  repeated map<string, google.protobuf.Any> properties = 3;
}

message Relationship {
  string name = 1;
  Node startNode = 2;
  Node endNode = 3;
  repeated map<string, google.protobuf.Any> properties = 4;
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

## REST APIs

We provide three types of methods:

1. GET: Obtain data from the database;
2. POST: Send/modify data to the database; 
3. DELETE: Delete data from the database.

These three methods are available for the all three core objects in the graph database.

For example, the following endpoints are available for graphs:

1. `GET /graphs`: obtain a list of graphs;
2. `GET /graphs/<graph_name>`: obtain a particular graph;
3. `POST /graphs/<graph_name>`: create a new graph in the database;
4. `DELETE /graph/<graph_name>`: delete an existing graph.

The following endpoints are available for nodes:
1. `GET /graph/<graph_name>/nodes`: obtain a list of nodes in a particular graph;
2. `GET /graph/<graph_name>/nodes/labels`: obtain a list of nodes in a particular graph by labels;
3. `GET /graph/<graph_name>/nodes/<node_name>`: get a particular node from a graph;
4. `POST /graphs/<graph_name>/nodes/<node_name>`: create a new node in the graph;
5. `DELETE /graph/<graph_name>/nodes/<node_name>`: delete an existing node;
6. `DELETE /graph/<graph_name>/nodes`: delete all nodes in the graph;
7. `DELETE /graph/<graph_name>/nodes/labels`: delete all nodes in the graph by labels.

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

## SDKs

We also provide SDKs such as Python SDK to our users.

First, initialize the client:

```python
client = tudb.Client(address=xxx)
```

We can then get the graphs and nodes similar to REST APIs:

```python
client.list_graphs()
graph = client.get_graph(name=xxx)

graph.list_nodes(labels=xxx)
client.list_nodes(graph_name=xxx, labels=xxx)
client.get_node(name=xxx)

graph.list_relationships()
client.list_relationships(graph_name=xxx)
client.get_relationship(name=xxx)
```
