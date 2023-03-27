#  Copyright 2022 The FeatHub Authors
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
import unittest
from abc import ABC
from datetime import timedelta
from typing import cast

from feathub.common.types import (
    Int32,
    Timestamp,
    Int64,
    String,
    Bool,
    Float32,
    Float64,
    VectorType,
    MapType,
)
from feathub.feature_tables.sources.datagen_source import (
    DataGenSource,
    RandomField,
    SequenceField,
    DEFAULT_BOUNDED_NUMBER_OF_ROWS,
)
from feathub.table.schema import Schema
from feathub.tests.feathub_it_test_base import FeathubITTestBase


class DataGenSourceTest(unittest.TestCase):
    def test_boundedness(self):
        source = DataGenSource(
            "source", Schema.new_builder().column("x", Int64).column("y", Int64).build()
        )
        self.assertFalse(source.is_bounded())

        source = DataGenSource(
            "source",
            Schema.new_builder().column("x", Int64).column("y", Int64).build(),
            number_of_rows=10,
        )
        self.assertTrue(source.is_bounded())

        source = DataGenSource(
            "source",
            Schema.new_builder().column("x", Int64).column("y", Int64).build(),
            field_configs={"x": SequenceField(start=1, end=10)},
        )
        self.assertTrue(source.is_bounded())

    def test_get_bounded_feature_table(self):
        source = DataGenSource(
            "source", Schema.new_builder().column("x", Int64).column("y", Int64).build()
        )
        self.assertFalse(source.is_bounded())

        bounded_source = source.get_bounded_view()
        self.assertTrue(bounded_source.is_bounded())
        self.assertEqual(
            DEFAULT_BOUNDED_NUMBER_OF_ROWS,
            cast(DataGenSource, bounded_source).number_of_rows,
        )

        source_json = source.to_json()
        source_json.pop("number_of_rows")
        bounded_source_json = bounded_source.to_json()
        bounded_source_json.pop("number_of_rows")

        self.assertEqual(source_json, bounded_source_json)


class DataGenSourceITTest(ABC, FeathubITTestBase):
    def test_data_gen_source(self):
        source = DataGenSource(
            name="datagen_src",
            number_of_rows=10,
            field_configs={
                "id": SequenceField(start=0, end=9),
                "val": RandomField(minimum=0, maximum=100),
            },
            schema=Schema(["id", "val", "ts"], [Int32, Int32, Timestamp]),
            timestamp_field="ts",
            timestamp_format="%Y-%m-%d %H:%M:%S",
        )

        df = self.client.get_features(features=source).to_pandas()

        self.assertEquals(10, df.shape[0])
        self.assertTrue((df["val"] >= 0).all() and (df["val"] <= 100).all())

    def test_num_of_rows_not_set(self):
        source = DataGenSource(
            name="datagen_src",
            rows_per_second=10,
            field_configs={
                "id": SequenceField(start=0, end=9),
                "val": RandomField(minimum=0, maximum=100),
            },
            schema=Schema(["id", "val", "ts"], [Int32, Int32, Timestamp]),
            timestamp_field="ts",
            timestamp_format="%Y-%m-%d %H:%M:%S",
        )

        df = self.client.get_features(features=source).to_pandas()

        self.assertEquals(10, df.shape[0])
        self.assertTrue((df["val"] >= 0).all() and (df["val"] <= 100).all())

    def test_num_of_rows_larger_than_sequence_field_size(self):
        source = DataGenSource(
            name="datagen_src",
            number_of_rows=100,
            field_configs={
                "id": SequenceField(start=0, end=9),
                "val": RandomField(minimum=0, maximum=100),
            },
            schema=Schema(["id", "val", "ts"], [Int32, Int32, Timestamp]),
            timestamp_field="ts",
            timestamp_format="%Y-%m-%d %H:%M:%S",
        )

        df = self.client.get_features(features=source).to_pandas()

        self.assertEquals(10, df.shape[0])
        self.assertTrue((df["val"] >= 0).all() and (df["val"] <= 100).all())

    def test_num_of_rows_smaller_than_sequence_field_size(self):
        source = DataGenSource(
            name="datagen_src",
            number_of_rows=10,
            field_configs={
                "id": SequenceField(start=0, end=99),
                "val": RandomField(minimum=0, maximum=100),
            },
            schema=Schema(["id", "val", "ts"], [Int32, Int32, Timestamp]),
            timestamp_field="ts",
            timestamp_format="%Y-%m-%d %H:%M:%S",
        )

        df = self.client.get_features(features=source).to_pandas()

        self.assertEquals(10, df.shape[0])
        self.assertTrue((df["val"] >= 0).all() and (df["val"] <= 100).all())

    # TODO: Unify the naming of test cases in FeathubITTestBase's child classes,
    #  so that when xxxProcessorITTest multi-inherit these child classes, test
    #  methods would not have their names collide and override each other.
    def test_all_supported_data_and_field_types(self):
        source = DataGenSource(
            name="datagen_src",
            number_of_rows=10,
            field_configs={
                "sequence_int32_value": SequenceField(start=0, end=9),
                "sequence_int64_value": SequenceField(start=0, end=9),
                "sequence_float32_value": SequenceField(start=0, end=9),
                "sequence_float64_value": SequenceField(start=0, end=9),
                "sequence_string_value": SequenceField(start=0, end=9),
            },
            schema=(
                Schema.new_builder()
                .column("random_string_value", String)
                .column("random_bool_value", Bool)
                .column("random_int32_value", Int32)
                .column("random_int64_value", Int64)
                .column("random_float32_value", Float32)
                .column("random_float64_value", Float64)
                .column("random_timestamp_value", Timestamp)
                .column("random_vector_value", VectorType(Int64))
                .column("random_map_value", MapType(Int64, String))
                .column("sequence_int32_value", Int32)
                .column("sequence_int64_value", Int64)
                .column("sequence_float32_value", Float32)
                .column("sequence_float64_value", Float64)
                .column("sequence_string_value", String)
                .build()
            ),
        )

        df = self.client.get_features(features=source).to_pandas()

        self.assertEquals(10, df.shape[0])

    def test_random_field_max_past(self):
        source = DataGenSource(
            name="datagen_src",
            rows_per_second=1000,
            field_configs={
                "id": SequenceField(start=0, end=999),
                "ts": RandomField(max_past=timedelta(seconds=1)),
            },
            schema=Schema(["id", "ts"], [Int32, Timestamp]),
            timestamp_field="ts",
            timestamp_format="%Y-%m-%d %H:%M:%S",
        )

        df = self.client.get_features(features=source).to_pandas()

        self.assertEquals(1000, df.shape[0])
        self.assertTrue((df["ts"].max() - df["ts"].min()) <= timedelta(seconds=1))

    def test_random_field_length(self):
        source = DataGenSource(
            name="datagen_src",
            number_of_rows=10,
            field_configs={
                "string_value": RandomField(length=101),
                "vector_value": RandomField(length=101),
            },
            schema=Schema(
                ["string_value", "vector_value"], [String, VectorType(Int64)]
            ),
        )

        df = self.client.get_features(features=source).to_pandas()

        self.assertEquals(10, df.shape[0])
        self.assertTrue((df["string_value"].str.len() == 101).all())
        self.assertTrue((df["vector_value"].str.len() == 101).all())
