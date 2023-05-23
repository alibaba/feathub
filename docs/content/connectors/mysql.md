# MySQL

FeatHub provides `MySQLSource` to read data from a MySQL table and `MySQLSink` to 
materialize feature view to a MySQL table. Currently, `MySQLSource` can only be used as 
an online store source.

## Supported Processors and Modes

<!-- TODO: Add description for supported versions -->
- Flink: Lookup<sup>1</sup>, Streaming Append, Streaming Upsert

1. Only supported in OnDemandFeatureView currently.

## Examples

Here are the examples of using `MySQLSource` and `MySQLSink`:

### Use as Streaming Append/Upsert Sink

If the feature view to be materialized has keys, the MySQLSink is in upsert mode 
otherwise it is in append mode.

```python
feature_view = DerivedFeatureView(...)

sink = MySQLSink(
    database="database",
    table="table",
    host="localhost",
    username="user",
    password="password",
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

source = MySQLSource(
    name="feature_table",
    database="database",
    table="table",
    schema=feature_table_schema,
    host="localhost",
    username="user",
    password="password",
    keys=["id"],
    timestamp_field="ts",
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
