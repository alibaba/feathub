# Protobuf Format

The [Parquet](https://parquet.apache.org/) format allows you to read and write
Parquet data.

## How to create a table with Parquet format

Here is an example to create a table using the FileSystem connector and Parquet
format.

If you are using Parquet format in Flink Processor, make sure you have
configured the environment variable HADOOP_CLASSPATH on both your Flink cluster
and Feathub client. Please refer to [Flink's document for Hive
connector](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/connectors/table/hive/overview/#supported-hive-versions)
for supported Hadoop & Hive versions.

Then you can use the Parquet format in filesystem connector as follows.

```python
schema = (
    Schema.new_builder()
    .column("id", types.Int64)
    .column("val", types.Int64)
    .column("ts", types.String)
    .build()
)

source = FileSystemSource(
    name="filesystem_source",
    path="/tmp",
    data_format="parquet",
    schema=schema,
)
```
