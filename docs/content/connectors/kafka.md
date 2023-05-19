# Kafka

FeatHub provides `KafkaSource` to read data from a Kafka topic and `KafkaSink` to 
materialize feature view to a Kafka topic.

## Supported Processors and Modes

- Flink: Streaming Scan, Streaming Append

## Supported format

- Flink: CSV, JSON, Protobuf

## Examples

Here are the examples of using `KafkaSource` and `KafkaSink`:

### Use as Streaming Append Sink

```python
feature_view = DerivedFeatureView(...)

sink = KafkaSink(
    bootstrap_server="bootstrap_server",
    topic="topic",
    key_format=None,
    value_format="json",
)

feathub_client.materialize_features(
    feature_descriptor=feature_view,
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

source = KafkaSource(
    name="kafka_source",
    bootstrap_server="bootstrap_server",
    topic="topic",
    key_format="json",
    value_format="json",
    schema=schema,
    consumer_group="consumer_group",
    keys=["key"],
    timestamp_field="timestamp",
    timestamp_format="%Y-%m-%d %H:%M:%S %z",
    startup_mode="earliest-offset",
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

- [FeatHub SlidingFeatureView Example](https://github.com/flink-extended/feathub-examples/tree/master/flink-sliding-feature-view): 
  This demo uses KafkaSource and KafkaSink to read input from Kafka, compute features, 
  and write the features to Kafka. 