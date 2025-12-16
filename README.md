# SearchHound

**SearchHound** is a research-grade distributed search engine designed to execute **relational joins at search time** across **Lucene-based indexes** using **Bloom-filter optimized distributed query planning**.

The project explores how large-scale search systems can natively support **relationship-aware queries** without pre-denormalization, enabling real-time joins over independently indexed data sets.

---

## Motivation

Traditional search engines (Lucene, Solr, Elasticsearch) are optimized for document-centric retrieval. Relational joins are typically avoided or pre-computed, leading to:

- Index duplication and denormalization
- Complex ingestion pipelines
- Stale or inconsistent relationships
- Limited query expressiveness

SearchHound addresses these limitations by introducing:

- **Distributed join execution**
- **Cost-based join planning**
- **Bloom-filter pruning**
- **Hybrid search + relational semantics**

---

## Core Concepts

### Distributed Join Search

SearchHound enables joins between independent Lucene indexes (or index shards) at query time. Each join is executed in a distributed fashion using a coordinator-leaf model.

### Bloom Filter Optimization

Bloom filters are generated from join keys and distributed across nodes to:

- Reduce network fan-out
- Prune non-matching documents early
- Minimize intermediate result sizes

### Hierarchical Query Planning

Queries are decomposed into execution stages using a cost-based planner that considers:

- Join cardinality
- Filter selectivity
- Bloom filter effectiveness
- Network and execution cost

---



## Architecture Overview



```
               +----------------------+
               |    SearchCoordinator |
               |     (Query Planner)  |
               +----------+-----------+
                          |
                          v
+----------+-----------+     +----------------------+
|     SearchLeaf       | ... |      SearchLeaf      |
|   (Lucene + Joins)   |     |    (Lucene + Joins)  |
+----------+-----------+     +----------------------+
                          |
                          v
               +----------------------+
               |  BloomFilterService  |
               +----------------------+
```

## Key Components

### SearchCoordinator

- Global query planner and execution coordinator
- Builds distributed execution plans
- Manages join ordering and Bloom filter propagation

### SearchLeaf

- Hosts Lucene indexes and joinable datasets
- Executes local query stages
- Applies Bloom filters for early pruning

### BloomFilterService

- Builds, compresses, and distributes Bloom filters
- Supports merging and reuse across query stages

### JoinCoordinatorService

- Executes distributed join workflows
- Aggregates partial results from SearchLeaf nodes

---

## Query Model

SearchHound uses a **platform-neutral query representation** that supports:

- Boolean filters
- Term and range queries
- Join expressions
- Nested and multi-stage joins

Example (conceptual):

```text
SELECT *
FROM documents d
JOIN users u ON d.owner_id = u.id
WHERE u.department = 'engineering'
AND d.visibility = 'public'
```



This query is translated into a distributed Lucene execution plan at runtime.

------

## Use Cases

- Relationship-aware enterprise search
- Security-aware document retrieval
- Hybrid search + analytics systems
- Research into distributed join optimization
- Lucene-based alternatives to search-backed RDBMS queries

------

## Project Status

SearchHound is currently:

- **Research-focused**
- **Actively evolving**
- **Not production-hardened**

APIs, execution strategies, and data models may change as experimentation continues.

------

## Getting Started

### Prerequisites

- Java 17+
- Maven or Gradle
- Lucene-based indexes (Solr or embedded Lucene)

### Build

```

mvn clean package
```

### Run (Prototype)

```

java -jar searchhound-coordinator.jar
```

(SearchLeaf nodes are started separately.)

------

## Design Goals

- No mandatory denormalization
- Join execution close to the data
- Minimal network amplification
- Explicit and explainable query plans
- Extensible execution model

------

## Non-Goals

- Replacing relational databases
- Full SQL compliance
- OLTP workloads
- Turn-key production deployment

------

## Related Projects

- **Lucene / Solr / Elasticsearch**
- **Searchbound** – platform-neutral search gateway
- **Lucentrix** – research and experimentation lab

------

## Contributing

Contributions are welcome, particularly in:

- Join planning algorithms
- Bloom filter optimization
- Cost modeling
- Query representation
- Performance instrumentation

Please open an issue or discussion before submitting major changes.

------

## License

This project is licensed under the **Apache License 2.0**.

------

## Disclaimer

SearchHound is a research project intended for experimentation and exploration. It should be evaluated carefully before use in any production environment.