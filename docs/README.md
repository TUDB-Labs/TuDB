[![CI](https://github.com/TUDB-Labs/TuDB/workflows/CI/badge.svg)](https://github.com/TUDB-Labs/TuDB/actions?query=event%3Apush+branch%3Amain)

# TuDB 

## What is TuDB?
TuDB is a cloud native graph database. TuDB aims to supports Hybrid Transactional and Analytical Processing (HTAP) workloads. It is Cypher compatible and features horizontal scalability, strong consistency, and high availability. TuDB aims to provide a unified graph process interface for constructing and managing graphs on different key-value storage engines, such as RocksDB, HBase, and TiKV.

## Quick Start 

### Building TuDB

```bash
mvn clean compile install
```

### Running the Test 
```bash
mvn clean install test
```

### Start a database instance 
```bash
./bin/start 
```

### Interactive Shell
```bash
./bin/cypher
```

## Community

### Get connected
We provide multiple channels to connect you to the community of the TuDB developers, users, and the general graph academic researchers:

* Our Slack channel
* Mail list 

## Configuration
Please refer to the [Configuration Guide](docs/Configuration.md) in the online documentation for an overview on how to configure TuDB.

## Contributing
Please review the [Contributing Guide](CONTRIBUTING.md) for information on how to get started contributing to the project.
