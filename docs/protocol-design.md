# Protocol Design

## Protocol Definitions

Object definitions:
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

message 
```

Generic query request
```

```


Query response
```
message QueryResponse {
  repeated map<string, google.protobuf.Any> results = 1;
  string message = 2;
  int32 exit_code = 3;
}
```
error
message
result

```
service GraphQueryService {
  rpc Query (QueryRequest) returns (stream QueryResponse) {
  }

  rpc QueryStatistics(QueryRequest) returns (stream QueryResponse) {
  }
}

message QueryRequest {
  repeated string labels = 1;
  repeated Relationship relationships = 2;
  map<string, string> properties = 3;
};

message Relationship {
  bool direction = 1;
  string type = 2;
}
```

## REST APIs

We provide three types of methods:
1. GET: Obtain data from the database;
2. POST: Send data to the database;
3. PUT: Modify existing data in the database;
4. DELETE: Delete data from the database.

These three methods are available for the following objects in the graph database:
1. Graphs;
2. Nodes;
3. Relationships.

For example, the following endpoints are available for graphs:

1. `GET /graphs`: obtain a list of graphs;
2. `GET /graphs/<graph_name>`: obtain a particular graph;
3. `POST /graphs/<graph_name>`: create a new graph in the database;
4. `PUT /graphs/<graph_name>`: modify an existing graph;

The following endpoints are available for nodes:
1. `GET /graph/<graph_name>/nodes`: obtain a list of nodes in a particular graph;
2. `GET /graph/<graph_name>/nodes/<node_name>`: get a particular node from a graph;

The following endpoints are available for relationships:
4. `GET /graph/<graph_name>/relationships`: get a list of relationships from a graph;
5. `GET /graph/<graph_name>/relationships/<relationship_name>`: get a particular relationship from a graph;
6. `DELETE /graph/<graph_name>`: delete an existing graph.

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

```python
client = tudb.Client(address=xxx)
client.list_graphs()
graph = client.get_graph(name=xxx)

graph.list_nodes()
client.list_nodes(graph_name=xxx)
client.get_node(node_name=xxx)
```