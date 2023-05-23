# Redis

FeatHub provides `RedisSource` to read data from a Redis database and
`RedisSink` to materialize feature view to a Redis database. The Redis service
should be deployed in standalone, master-slave, or cluster mode. Currently,
`RedisSource` can only be used as an online store source.

<!-- TODO: Add document describing the structure of data saved to Redis. -->

## Supported Processors and Usages

- Flink: Lookup<sup>1</sup>, Streaming Upsert

1. Only supported in OnDemandFeatureView currently.

## Examples

Here are the examples of using `RedisSource` and `RedisSink`:

### Use as Streaming Upsert Sink

 `RedisSink` works in upsert mode and requires that the feature view to be
 materialized must have keys.

```python
feature_view = DerivedFeatureView(
    keys=["user_id", "item_id"],
    ...
)

sink = RedisSink(
    host="host",
    port=6379,
    username="username",
    password="password",
    db_num=0,
    key_expr='CONCAT_WS(":", __NAMESPACE__, __KEYS__, __FEATURE_NAME__)',
    # key_expr can also be configured as follows, and the effect is the same.
    # key_expr='CONCAT_WS(":", __NAMESPACE__, user_id, item_id, __FEATURE_NAME__)',
)

feathub_client.materialize_features(
    feature_descriptor=feature_view,
    sink=sink,
    allow_overwrite=True,
).wait(30000)
```

### Use as Lookup Source for OnDemandFeatureView

```python
feature_table_schema = (
    Schema.new_builder()
    .column("id", types.Int64)
    .column("feature_1", types.Int64)
    .column("ts", types.String)
    .build()
)

source = RedisSource(
    name="feature_table",
    schema=feature_table_schema,
    keys=["user_id", "item_id"],
    host="host",
    port=6379,
    timestamp_field="ts",
    key_expr='CONCAT_WS(":", __NAMESPACE__, __KEYS__, __FEATURE_NAME__)',
    # key_expr can also be configured as follows, and the effect is the same.
    # key_expr='CONCAT_WS(":", __NAMESPACE__, user_id, item_id, __FEATURE_NAME__)',
)

on_demand_feature_view = OnDemandFeatureView(
    name="on_demand_feature_view",
    features=[
        "feature_table.feature_1",
    ],
    keep_source_fields=True,
    request_schema=Schema.new_builder().column("id", types.Int64).build(),
)

feathub_client.build_features([source, on_demand_feature_view])

request_df = pd.DataFrame(np.array([[2], [3]]), columns=["id"])

online_features = feathub_client.get_online_features(
    request_df=request_df,
    feature_view=on_demand_feature_view,
)
```
