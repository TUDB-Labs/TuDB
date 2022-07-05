# Protocol Design

## Protocol Definitions

Generic query request
```protobuf

```


Query response
```protobuf
message QueryResponse {
  string result = 1;
  string message = 2;
  string status = 3;
}
```
error
message
result

```protobuf
service TuQueryService {
  rpc Query (QueryRequest) returns (stream QueryResponse) {
  }

  rpc QueryStatistics(QueryRequest) returns (stream QueryResponse) {
  }
}

message QueryRequest {
  repeated string labels = 1;
  repeated Relationship relationships = 2;
  map<string, string> properties;
};

message Relationship {
  bool direction = 1;
  string type = 2;
}
```

## REST APIs

Server statistics for each requests:
ref: https://docs.tigergraph.com.cn/dev/restpp-api/built-in-endpoints#get-statistics-duan-dian
```
GET /statistics
```
```
GET /graphs
GET /graphs/<graph_name>
GET /graphs/<graph_name>/<query_key>
POST /graphs/<graph_name>/

GET /graph/<graph_name>/nodes
GET /graph/<graph_name>/nodes/<node_name>
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