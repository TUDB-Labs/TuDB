# Performance Report

## Experiment Setup

### Environment
OS: CentOS7  
CPU: 26 cores  
DISK: SSD  
Mem: 376G  
JDK-version: 1.8  
Scala-version: 2.12.8  

### Dataset
[LDBC-SNB](https://ldbcouncil.org/)

| Name |  # Nodes  | # Relationships |
|:----:|:---------:|:---------------:|
| SF1  |  3115527  |    17236389     |
| SF3  |  8879918  |    50728269     |
| SF10 | 28125740  |    166346601    |

### Method
Node test: choose the first node from each label, then test different API avg time cost.  
relation test: choose the first end node from each relation type, then test different API avg time cost.  

Notice: 
1. **allNodes**: test the time cost of scan all node data in db.
2. **findInRelations**: test the time cost of return the first 10 relations of this node.

## Performance Report
### SF1 Performance
| NodeStoreAPI       | avg time cost | RelationStoreApi       | avg time cost |
|:-------------------|:--------------|:-----------------------|:--------------|
| allNodes           | 2444   ms     | findInRelations        | 486299 us     |
| getNodeById        | 187896 us     | getRelationById        | 11987  us     |
| nodeSetProperty    | 961045 us     | relationSetProperty    | 202494 us     |
| nodeRemoveProperty | 773296 us     | relationRemoveProperty | 158453 us     |
| deleteNode         | 226737 us     | deleteRelation         | 36684  us     |
| addNode            | 35091  us     | addRelation            | 32427  us     |

### SF3 Performance
| NodeStoreAPI       | avg time cost | RelationStoreApi       | avg time cost |
|:-------------------|:--------------|:-----------------------|:--------------|
| allNodes           | 7145   ms     | findInRelations        | 472155 us     |
| getNodeById        | 368505 us     | getRelationById        | 15265  us     |
| nodeSetProperty    | 941363 us     | relationSetProperty    | 198322 us     |
| nodeRemoveProperty | 644449 us     | relationRemoveProperty | 171554 us     |
| deleteNode         | 169693 us     | deleteRelation         | 33459  us     |
| addNode            | 32525  us     | addRelation            | 32301  us     |

### SF10 Performance
| NodeStoreAPI       | avg time cost | RelationStoreApi       | avg time cost |
|:-------------------|:--------------|:-----------------------|:--------------|
| allNodes           | 23491  ms     | findInRelations        | 589703 us     |
| getNodeById        | 272012 us     | getRelationById        | 18101  us     |
| nodeSetProperty    | 941371 us     | relationSetProperty    | 193019 us     |
| nodeRemoveProperty | 865621 us     | relationRemoveProperty | 179682 us     |
| deleteNode         | 197920 us     | deleteRelation         | 37630  us     |
| addNode            | 34704  us     | addRelation            | 36308  us     |