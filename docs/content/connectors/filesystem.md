# Filesystem

FeatHub provides `FileSystemSource` to read data from file systems and `FileSystemSink`
to materialize feature view to file systems.

## Supported Processors and Modes

- Local: Batch Scan, Batch Append
- Flink: Streaming Scan, Streaming Append
- Spark: Batch Scan, Batch Append

## Supported file system types

Various processors support different types of file systems, including popular ones such 
as local, HDFS, Amazon S3, and Aliyun OSS. Below are the file system types that have 
been tested and supported by each processor.

- Local: local, HDFS<sup>1</sup>
- Flink: local, HDFS, Amazon S3, Aliyun OSS
- Spark: local, HDFS

1. Supported via Spark's local mode.

## Examples

Here are the examples of using `FileSystemSource` and `FileSystemSink`:

### Use as Batch Append Sink

```python
feature_view = DerivedFeatureView(...)

result_table = feathub_client.get_features(feature_view)

sink = FileSystemSink(
    path="hdfs://namenode:8020/dummy/path",
    data_format="csv"
)

result_table.execute_insert(
    sink=sink, 
    allow_overwrite=True
).wait(30000)
```

### Use as Batch Scan Source

```python
schema = (
    Schema.new_builder()
    ...
    .build()
)

source = FileSystemSource(
    name="filesystem_source",
    path="hdfs://namenode:8020/dummy/path",
    data_format="csv",
    schema=schema,
    keys=["key"],
    timestamp_field="timestamp",
    timestamp_format="%Y-%m-%d %H:%M:%S",
)

feature_view = DerivedFeatureView(
    name="feature_view",
    source=source,
    features=[
        ...
    ],
)
```

## Additional Resources

- [FeatHub ReadWriteHDFS Example](https://github.com/flink-extended/feathub-examples/tree/master/flink-read-write-hdfs): 
  This demo uses FileSystemSource and FileSystemSink to read input from HDFS, compute features, 
  and write the features back to HDFS. 
