# Feathub Tutorial

This tutorial shows how to use Feathub to achieve the following objectives:
- Define, extract, transform and materialize features into feature stores.
- Transform features into Pandas dataframe for offline training.
- Materialize features into online feature store.
- Fetch features with on-demand feature transformation from online feature store
  for online feature serving.

This tutorial shows these Feathub capabilities by walking you through
[nyc_taxi.py](./../python/feathub/examples/nyc_taxi.py), which trains a
GradientBoostingRegressor model on the NYC Taxi Records
[dataset](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) to
predict taxi fares, evaluates the prediction accuracy, and materialize features
into online feature store for online feature serving.

See [README](./../README.md#quickstart) for the instruction to run this demo.

## Specify source dataset

```python
source = FileSource(
    name="source_1",
    path=source_file_path,
    file_format="csv",
    timestamp_field="lpep_dropoff_datetime",
    timestamp_format="%Y-%m-%d %H:%M:%S",
)
```


## Define features as transformations on the source dataset

```python
f_trip_time_duration = Feature(
    name="f_trip_time_duration",
    dtype=types.Int32,
    transform="unix_timestamp(lpep_dropoff_datetime) - "
    "unix_timestamp(lpep_pickup_datetime)",
)

f_location_avg_fare = Feature(
    name="f_location_avg_fare",
    dtype=types.Float32,
    transform=WindowAggTransform(
        expr="cast_float(fare_amount)",
        agg_func="AVG",
        group_by_keys=["DOLocationID"],
        window_size=timedelta(days=90),
    ),
)

f_location_max_fare = Feature(
    name="f_location_max_fare",
    dtype=types.Float32,
    transform=WindowAggTransform(
        expr="cast_float(fare_amount)",
        agg_func="MAX",
        group_by_keys=["DOLocationID"],
        window_size=timedelta(days=90),
    ),
)

f_location_total_fare_cents = Feature(
    name="f_location_total_fare_cents",
    dtype=types.Float32,
    transform=WindowAggTransform(
        expr="cast_float(fare_amount * 100)",
        agg_func="SUM",
        group_by_keys=["DOLocationID"],
        window_size=timedelta(days=90),
    ),
)

feature_view_1 = DerivedFeatureView(
    name="feature_view_1",
    source=source,
    features=[
        f_location_avg_fare,
        f_location_max_fare,
        f_location_total_fare_cents,
    ],
    keep_source_fields=True,
)

f_trip_time_rounded = Feature(
    name="f_trip_time_rounded",
    dtype=types.Float32,
    transform="f_trip_time_duration / 10",
    input_features=[f_trip_time_duration],
)

f_is_long_trip_distance = Feature(
    name="f_is_long_trip_distance",
    dtype=types.Bool,
    transform="cast_float(trip_distance)>30",
)

feature_view_2 = DerivedFeatureView(
    name="feature_view_2",
    source="feature_view_1",
    features=[
        "f_location_avg_fare",
        f_trip_time_rounded,
        f_is_long_trip_distance,
        "f_location_total_fare_cents",
    ],
    keep_source_fields=True,
)

client.build_features(features_list=[feature_view_1, feature_view_2])
```

## Transform features into Pandas DataFrame for offline training.

```python
train_df = client.get_features(features).to_pandas()
```


## Materialize features into online feature store

```python
sink = OnlineStoreSink(
    store_type="memory",
    table_name="table_name_1",
)
selected_features = DerivedFeatureView(
    name="feature_view_3",
    source="feature_view_2",
    features=["f_location_avg_fare", "f_location_max_fare"],
)
client.build_features([selected_features])

job = client.materialize_features(
    features=selected_features,
    sink=sink,
    start_datetime=datetime(2020, 1, 1),
    end_datatime=datetime(2020, 5, 20),
    allow_overwrite=True,
)
job.wait(timeout_ms=10000)
```

## Fetch features from online feature store with on-demand transformations

```python
source = OnlineStoreSource(
    name="online_store_source",
    keys=["DOLocationID"],
    store_type="memory",
    table_name="table_name_1",
)
on_demand_feature_view = OnDemandFeatureView(
    name="on_demand_feature_view",
    features=[
        "online_store_source.f_location_avg_fare",
        "online_store_source.f_location_max_fare",
        Feature(
            name="max_avg_ratio",
            dtype=types.Float32,
            transform="f_location_max_fare / f_location_avg_fare",
        ),
    ],
)
client.build_features([source, on_demand_feature_view])

request_df = pd.DataFrame(np.array([[247]]), columns=["DOLocationID"])
online_features = client.get_online_features(
    request_df=request_df,
    feature_view=on_demand_feature_view,
)
```

