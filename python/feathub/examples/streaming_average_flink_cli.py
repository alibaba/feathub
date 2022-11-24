#  Copyright 2022 The Feathub Authors
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
from datetime import timedelta

from feathub.common.types import Int32, Timestamp, Float64
from feathub.feathub_client import FeathubClient
from feathub.feature_tables.sinks.print_sink import PrintSink
from feathub.feature_tables.sources.datagen_source import (
    DataGenSource,
    RandomField,
)
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature import Feature
from feathub.feature_views.transforms.over_window_transform import OverWindowTransform
from feathub.table.schema import Schema


def main() -> None:
    client = FeathubClient(
        props={
            "processor": {
                "type": "flink",
                "flink": {"deployment_mode": "cli"},
            },
            "online_store": {
                "types": ["memory"],
                "memory": {},
            },
            "registry": {
                "type": "local",
                "local": {
                    "namespace": "default",
                },
            },
            "feature_service": {
                "type": "local",
                "local": {},
            },
        }
    )

    source = DataGenSource(
        name="datagen_src",
        rows_per_second=1,
        field_configs={
            "id": RandomField(minimum=0, maximum=9),
            "val": RandomField(minimum=0, maximum=100),
        },
        schema=Schema.new_builder()
        .column("id", Int32)
        .column("val", Int32)
        .column("ts", Timestamp)
        .build(),
        keys=["id"],
        timestamp_field="ts",
    )

    feature_view = DerivedFeatureView(
        name="feature_view",
        source=source,
        features=[
            Feature(
                name="avg_val",
                dtype=Float64,
                transform=OverWindowTransform(
                    expr="val",
                    agg_func="AVG",
                    window_size=timedelta(minutes=1),
                    group_by_keys=["id"],
                    limit=10,
                ),
            ),
            Feature(name="val_plus_one", dtype=Int32, transform="val + 1"),
        ],
    )

    sink = PrintSink()
    client.materialize_features(feature_view, sink=sink, allow_overwrite=True)


if __name__ == "__main__":
    main()
