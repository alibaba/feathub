# Copyright 2022 The Feathub Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import pandas as pd
import numpy as np
from datetime import timedelta, datetime
from pathlib import Path
from os import path
from math import sqrt

from feathub.feature_views.feature_view import FeatureView
from feathub.table.schema import Schema

sys.path.append(str(Path(__file__).parent.parent.parent.resolve()))

from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingRegressor

from feathub.feature_tables.sources.file_system_source import FileSystemSource
from feathub.feature_tables.sinks.online_store_sink import OnlineStoreSink
from feathub.feature_views.feature import Feature
from feathub.feature_views.on_demand_feature_view import OnDemandFeatureView
from feathub.common import types
from feathub.feature_tables.sources.online_store_source import OnlineStoreSource
from feathub.feathub_client import FeathubClient
from feathub.feature_views.transforms.over_window_transform import OverWindowTransform
from feathub.feature_views.derived_feature_view import DerivedFeatureView


def main() -> None:
    client = FeathubClient(
        config={
            "processor": {
                "processor_type": "local",
                "local": {},
            },
            "online_store": {
                "memory": {},
            },
            "registry": {
                "registry_type": "local",
                "local": {
                    "namespace": "default",
                },
            },
            "feature_service": {
                "service_type": "local",
                "local": {},
            },
        }
    )

    run_nyc_taxi_example(client)


def run_nyc_taxi_example(client: FeathubClient) -> None:
    # Define features as transformations on the source dataset
    features = build_features(client)

    # Transform features into Pandas DataFrame for offline training.
    train_df = client.get_features(features).to_pandas()

    # Train a model using the dataset and evaluate the model accuracy.
    train_and_evaluate_accuracy(train_df)

    # Materialize features into online feature store.
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
        end_datetime=datetime(2020, 5, 20),
        allow_overwrite=True,
    )
    job.wait(timeout_ms=10000)

    # Fetch features from online feature store with on-demand transformations.
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

    print(online_features)


def build_features(client: FeathubClient) -> FeatureView:
    # source_file_path = "https://azurefeathrstorage.blob.core.windows.net/public" \
    #                    "/sample_data/green_tripdata_2020-04_with_index.csv"
    source_file_path = path.join(Path(__file__).parent.resolve(), "sample_data.csv")

    schema = Schema(
        field_names=[
            "trip_id",
            "VendorID",
            "lpep_pickup_datetime",
            "lpep_dropoff_datetime",
            "store_and_fwd_flag",
            "RatecodeID",
            "PULocationID",
            "DOLocationID",
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "ehail_fee",
            "improvement_surcharge",
            "total_amount",
            "payment_type",
            "trip_type",
            "congestion_surcharge",
        ],
        field_types=[
            types.Int64,
            types.Float64,
            types.String,
            types.String,
            types.String,
            types.Float64,
            types.Int64,
            types.Int64,
            types.Float64,
            types.Float64,
            types.Float64,
            types.Float64,
            types.Float64,
            types.Float64,
            types.Float64,
            types.Float64,
            types.Float64,
            types.Float64,
            types.Float64,
            types.Float64,
            types.Float64,
        ],
    )

    source = FileSystemSource(
        name="source_1",
        path=source_file_path,
        data_format="csv",
        schema=schema,
        timestamp_field="lpep_dropoff_datetime",
        timestamp_format="%Y-%m-%d %H:%M:%S",
    )

    f_trip_time_duration = Feature(
        name="f_trip_time_duration",
        dtype=types.Int32,
        transform="UNIX_TIMESTAMP(CAST(lpep_dropoff_datetime AS STRING)) - "
        "UNIX_TIMESTAMP(CAST(lpep_pickup_datetime AS STRING))",
    )

    # f_trip_distance = Feature(
    #     name="f_trip_distance", dtype=types.Float32, transform="trip_distance"
    # )
    #
    # f_day_of_week = Feature(
    #     name="f_day_of_week",
    #     dtype=types.Int32,
    #     transform="dayofweek(lpep_dropoff_datetime)",
    # )
    #
    # f_trip_time_distance = Feature(
    #     name="f_trip_time_distance",
    #     dtype=types.Float32,
    #     transform="trip_distance * f_trip_duration",
    #     input_features=[f_trip_distance, f_trip_time_duration],
    # )

    f_location_avg_fare = Feature(
        name="f_location_avg_fare",
        dtype=types.Float32,
        transform=OverWindowTransform(
            expr="CAST(fare_amount AS FLOAT)",
            agg_func="AVG",
            group_by_keys=["DOLocationID"],
            window_size=timedelta(days=90),
        ),
    )

    f_location_max_fare = Feature(
        name="f_location_max_fare",
        dtype=types.Float32,
        transform=OverWindowTransform(
            expr="CAST(fare_amount AS FLOAT)",
            agg_func="MAX",
            group_by_keys=["DOLocationID"],
            window_size=timedelta(days=90),
        ),
    )

    f_location_total_fare_cents = Feature(
        name="f_location_total_fare_cents",
        dtype=types.Float32,
        transform=OverWindowTransform(
            expr="CAST(fare_amount * 100 AS FLOAT)",
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
        transform="CAST(trip_distance AS FLOAT)>30",
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

    return feature_view_2


def train_and_evaluate_accuracy(train_dataset: pd.DataFrame) -> None:
    final_df = train_dataset

    final_df.drop(
        ["lpep_pickup_datetime", "lpep_dropoff_datetime", "store_and_fwd_flag"],
        axis=1,
        inplace=True,
        errors="ignore",
    )

    final_df.fillna(0, inplace=True)
    final_df["fare_amount"] = final_df["fare_amount"].astype("float64")

    train_x, test_x, train_y, test_y = train_test_split(
        final_df.drop(["fare_amount"], axis=1),
        final_df["fare_amount"],
        test_size=0.2,
        random_state=42,
    )
    model = GradientBoostingRegressor()
    model.fit(train_x, train_y)

    y_predict = model.predict(test_x)

    y_actual = test_y.values.flatten().tolist()
    rmse = sqrt(mean_squared_error(y_actual, y_predict))

    sum_actuals = sum_errors = 0

    for actual_val, predict_val in zip(y_actual, y_predict):
        abs_error = actual_val - predict_val
        if abs_error < 0:
            abs_error = abs_error * -1

        sum_errors = sum_errors + abs_error
        sum_actuals = sum_actuals + actual_val

    mean_abs_percent_error = sum_errors / sum_actuals

    print(f"Model MSE {rmse}")
    print(f"Model MAPE {mean_abs_percent_error}")
    print(f"Model Accuracy: {1 - mean_abs_percent_error}")


if __name__ == "__main__":
    main()
