## Background

### Current design

+ `TuStoreContext` stores reference to:
  + `NodeStoreAPI`
  + `RelationshipStoreAPI`
+ `storage` package:
  + details about the underline storage system, such as rocksdb storage
+ `relationship` package:
  + `RelationshipStoreSPI` and `RelationshipStoreAPI` (extends `RelationshipStoreSPI`)
  + `Relationship` defines a relationship
    + `StoredRelationship`
    + `StoredRelationshipWithProperty`
    + `TuRelationship`
    + `LynxRelationshipId`
    + `RelationshipDirectionStore`
    + `RelationshipLabelIndex`
    + `RelationshipPropertyStore`
+ `node` package:
  + `StoredNode`
  + `StoredNodeWithProperty`
  + `LynxNodeId`
  + `TuNode`
  + `LazyTuNode`
  + `NodeLabelStore`
  + `NodeStore`
  + `NodeStoreSPI` and `NodeStoreAPI` (extends `NodeStoreSPI`)
+ `index` package:
  + `EmptyIndexServerImpl`, `MemoryIndexServerImpl` and `RocksIndexServerImpl`
  + `IndexStoreKVImpl`
  + `PropertyIndex`, `PropertyIndexStore`
+ `meta` package:
  + `ConfigNameMap`: object wraps property name and its value
  + `DbNameMap`: object wraps property name and its value
  + `IdGenerator`
  + `NameStore`: store information about an `id` and a string map
  + `NodeLabelNameStore`: extends `NameStore`
  + `PropertyNameStore`: extends `NameStore`
  + `RelationshipTypeNameStore`: extends `NameStore`
  + `TuDBStatistics`
  + `TypeManager`: aliases to different types of objects

Problems:

+ Graph abstractions are coupled with storage abstractions.
+ API calls are scattered around a lot of files which makes it hard to maintain.

## Proposed design

+ Storage abstraction wraps underline storage.
+ Graph abstraction wraps node, relationship, meta of a graph, it has no directly
interaction with storage
+ Engine abstraction wraps graph and storage and it provides services to other packages.

### Storage

The storage packages is similar to current design

### Graph

+ Node
  + Label
  + Properties
+ Relationship:
  + Label
  + Properties
+ Meta
+ Serializer

### Engine

It holds references to a graph and a storage.

+ Serialize data to bytes and store them
+ Deserialize from data read from storage
+ Provide CRUD support using graph and storage handler

## Others

+ Reference is not restricted, such `NodeStoreAPI` and `RelationshipStoreAPI` is
created and used directly.
+ Other functions are called in a lot of places.

