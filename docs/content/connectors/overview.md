# Overview

Connectors can be used to construct a FeatureTable in FeatHub. It can be a 
source to access and interpret a table of feature values from an offline or online 
feature store, or as a sink to locate and write a table of feature values to an offline 
or online feature store. 

## Connector Modes

A source can operate on one of the following modes:
- Batch Scan: Scan a bounded table from an external system for batch processing.
- Streaming Scan: Scan a bounded or unbounded append-only table from an external system 
  for stream processing.
- Streaming CDC: Consume the changelog stream from an external system for stream 
  processing.
- Lookup: Join the latest value from an external system at processing time.

A Sink can operate on one of the following modes:
- Batch Append: Write an append-only batch table to an external system.
- Streaming Append: Write an append-only streaming table to an external system.
- Streaming Upsert: Write an upsert streaming table to an external system.


The tables below describe the supported connectors and modes for different processors, 
where "Y" means supported, "Y/N" means partially supported, "N" means unsupported.

### Connectors and Modes Supported by LocalProcessor

| Connector\Modes             | Batch Scan | Streaming Scan | Streaming CDC | Lookup          | Batch Append | Streaming Append | Streaming Upsert |
|-----------------------------|------------|----------------|---------------|-----------------|--------------|------------------|------------------|
| [FileSystem](filesystem.md) | Y          | N              | N             | N               | Y            | N                | N                |

### Connectors and Modes Supported by FlinkProcessor

| Connector\Modes             | Batch Scan | Streaming Scan | Streaming CDC | Lookup          | Batch Append | Streaming Append | Streaming Upsert |
|-----------------------------|------------|----------------|---------------|-----------------|--------------|------------------|------------------|
| [FileSystem](filesystem.md) | N          | Y              | N             | N               | N            | Y                | N                |
| [MySQL](mysql.md)           | N          | N              | N             | Y/N<sup>1</sup> | N            | Y                | Y                |
| [Kafka](kafka.md)           | N          | Y              | N             | N               | N            | Y                | N                |
| [Redis](redis.md)           | N          | N              | N             | Y/N<sup>1</sup> | N            | N                | Y                |

1. Only supported in OnDemandFeatureView currently.

### Connectors and Modes Supported by SparkProcessor

| Connector\Modes             | Batch Scan | Streaming Scan | Streaming CDC | Lookup          | Batch Append | Streaming Append | Streaming Upsert |
|-----------------------------|------------|----------------|---------------|-----------------|--------------|------------------|------------------|
| [FileSystem](filesystem.md) | Y          | N              | N             | N               | Y            | N                | N                |
