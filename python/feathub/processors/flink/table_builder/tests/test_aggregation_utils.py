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
from datetime import timedelta

from pyflink.table import DataTypes

from feathub.common.exceptions import FeathubException
from feathub.common.types import Int32
from feathub.feature_views.feature import Feature
from feathub.feature_views.transforms.over_window_transform import OverWindowTransform
from feathub.feature_views.transforms.sliding_window_transform import (
    SlidingWindowTransform,
)
from feathub.processors.flink.table_builder.aggregation_utils import (
    AggregationFieldDescriptor,
)


class AggregationUtilsTest(unittest.TestCase):
    def test_aggregation_descriptor(self):

        sliding_feature = Feature(
            name="feature_1",
            transform=SlidingWindowTransform(
                expr="a",
                agg_func="COUNT",
                window_size=timedelta(seconds=10),
                step_size=timedelta(seconds=1),
                group_by_keys=["id"],
                filter_expr="a > 100",
                limit=10,
            ),
            dtype=Int32,
        )

        descriptor = AggregationFieldDescriptor.from_feature(sliding_feature)

        self.assertEqual("feature_1", descriptor.field_name)
        self.assertEqual("`a`", descriptor.expr)
        self.assertEqual(DataTypes.INT(), descriptor.field_data_type)
        self.assertEqual(timedelta(seconds=10), descriptor.window_size)
        self.assertEqual("`a` > 100", descriptor.filter_expr)
        self.assertEqual(10, descriptor.limit)

        over_feature = Feature(
            name="feature_1",
            transform=OverWindowTransform(
                expr="a",
                agg_func="COUNT",
                window_size=timedelta(seconds=10),
                group_by_keys=["id"],
                filter_expr="a > 100",
                limit=10,
            ),
            dtype=Int32,
        )

        descriptor = AggregationFieldDescriptor.from_feature(over_feature)

        self.assertEqual("feature_1", descriptor.field_name)
        self.assertEqual("`a`", descriptor.expr)
        self.assertEqual(DataTypes.INT(), descriptor.field_data_type)
        self.assertEqual(timedelta(seconds=10), descriptor.window_size)
        self.assertEqual("`a` > 100", descriptor.filter_expr)
        self.assertEqual(10, descriptor.limit)

        expression_feature = Feature(
            name="feature_1",
            transform="a + 1",
            dtype=Int32,
        )

        with self.assertRaises(FeathubException):
            AggregationFieldDescriptor.from_feature(expression_feature)
