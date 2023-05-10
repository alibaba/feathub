# Hive

FeatHub provides `HiveSource` to read data from a Hive table and `HiveSink` to
materialize feature view to a Hive table.

## Supported Processors and Modes

- Flink<sup>1</sup>: Streaming Scan, Streaming Append

1. Only supports Hadoop 3.1.x

## Configuring Hive connector for FlinkProcessor

Before using Hive connector in Flink Processor, make sure you have configured
the hadoop and hive environment on both your Flink cluster and PyFlink client.
Please refer to [Flink's
document](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/connectors/table/hive/overview/)
for configuration guidelines.

## Examples

Here are the examples of using `HiveSource` and `HiveSink`:

### Use as Streaming Append Sink

```python
feature_view = DerivedFeatureView(...)

hive_sink = HiveSink(
    database="default",
    table="table",
    hive_catalog_conf_dir=".",
    processor_specific_props={
        'sink.partition-commit.policy.kind': 'metastore,success-file',
    }
)

feathub_client.materialize_features(
    features=feature_view,
    sink=sink,
    allow_overwrite=True,
).wait(30000)
```

### Use as Streaming Scan Source

```python
schema = (
    Schema.new_builder()
    ...
    .build()
)

source = HiveSource(
    name="hive_source",
    database="default",
    table="table",
    schema=schema,
    keys=["key"],
    hive_catalog_conf_dir=".",
    timestamp_field="timestamp",
    timestamp_format="%Y-%m-%d %H:%M:%S %z",
)

feature_view = DerivedFeatureView(
    name="feature_view",
    source=source,
    features=[
        ...
    ],
)
```