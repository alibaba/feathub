# Copyright 2022 The FeatHub Authors
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
import unittest
from typing import Optional, Dict

from feathub.common.exceptions import FeathubConfigurationException
from feathub.feathub_client import FeathubClient
from feathub.feature_tables.tests.test_black_hole_sink import BlackHoleSinkITTest
from feathub.feature_tables.tests.test_datagen_source import DataGenSourceITTest
from feathub.feature_tables.tests.test_file_system_source_sink import (
    FileSystemSourceSinkITTest,
)
from feathub.feature_tables.tests.test_print_sink import PrintSinkITTest
from feathub.feature_views.tests.test_derived_feature_view import (
    DerivedFeatureViewITTest,
)
from feathub.feature_views.transforms.tests.test_expression_transform import (
    ExpressionTransformITTest,
)
from feathub.feature_views.transforms.tests.test_join_transform import (
    JoinTransformITTest,
)
from feathub.feature_views.transforms.tests.test_over_window_transform import (
    OverWindowTransformITTest,
)
from feathub.feature_views.transforms.tests.test_python_udf_transform import (
    PythonUDFTransformITTest,
)
from feathub.processors.spark.spark_processor import SparkProcessor
from feathub.registries.local_registry import LocalRegistry
from feathub.tests.test_get_features import GetFeaturesITTest
from feathub.tests.test_online_features import OnlineFeaturesITTest


class SparkProcessorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.registry = LocalRegistry(props={})

    def test_native_config_conflict(self):
        with self.assertRaises(FeathubConfigurationException):
            SparkProcessor(
                props={
                    "processor.spark.master": "local[1]",
                    "processor.spark.native.spark.master": "local[2]",
                },
                registry=self.registry,
            )

        with self.assertRaises(FeathubConfigurationException):
            SparkProcessor(
                props={
                    "processor.spark.master": "local[1]",
                    "processor.spark.native.spark.sql.session.timeZone": "Asia/Beijing",
                },
                registry=self.registry,
            )

    def test_none_master_config(self):
        with self.assertRaises(FeathubConfigurationException) as cm:
            SparkProcessor(
                props={
                    "processor.spark.master": None,
                },
                registry=self.registry,
            )

        self.assertIn("cannot be None", cm.exception.args[0])


class SparkProcessorITTest(
    BlackHoleSinkITTest,
    DataGenSourceITTest,
    ExpressionTransformITTest,
    FileSystemSourceSinkITTest,
    PrintSinkITTest,
    OverWindowTransformITTest,
    PythonUDFTransformITTest,
    OnlineFeaturesITTest,
    JoinTransformITTest,
    GetFeaturesITTest,
    DerivedFeatureViewITTest,
):
    __test__ = True

    @classmethod
    def setUpClass(cls) -> None:
        cls.invoke_all_base_class_setupclass()

    def setUp(self):
        self.invoke_base_class_setup()

    def tearDown(self) -> None:
        self.invoke_base_class_teardown()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.invoke_all_base_class_teardownclass()

    def get_client(self, extra_config: Optional[Dict] = None) -> FeathubClient:
        return self.get_client_with_local_registry(
            {
                "type": "spark",
                "spark": {
                    "master": "local[1]",
                },
            },
            extra_config,
        )

    # TODO: Add back following test cases after SparkProcessor supports aggregations
    #  on both limited and timed window
    def test_over_window_on_join_field(self):
        pass

    def test_over_window_transform_value_counts(self):
        pass

    def test_over_window_transform_row_num(self):
        pass

    def test_over_window_transform_with_different_criteria(self):
        pass

    def test_over_window_transform_with_window_size_and_limit(self):
        pass

    def test_over_window_transform_first_last_value_with_window_size_and_limit(self):
        pass

    def test_over_window_transform_filter_expr_with_window_size_and_limit(self):
        pass

    def test_bounded_left_table_join_unbounded_right_table(self):
        pass

    def test_over_window_transform_count_with_limit(self):
        pass

    # TODO: Support protobuf format for SparkProcessor FileSystem connector.
    def test_local_file_system_protobuf_source_sink(self):
        pass

    def test_protobuf_all_types(self):
        pass
