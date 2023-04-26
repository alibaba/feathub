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
import re
import unittest
from typing import Optional, Dict, List, Type
from unittest.mock import Mock

from feathub.feathub_client import FeathubClient
from feathub.feature_tables.feature_table import FeatureTable
from feathub.feature_tables.tests.test_black_hole_sink import BlackHoleSinkITTest
from feathub.feature_tables.tests.test_datagen_source import DataGenSourceITTest
from feathub.feature_tables.tests.test_file_system_source_sink import (
    FileSystemSourceSinkITTest,
)
from feathub.feature_tables.tests.test_print_sink import PrintSinkITTest
from feathub.feature_views.tests.test_derived_feature_view import (
    DerivedFeatureViewITTest,
)
from feathub.feature_views.tests.test_sliding_feature_view import (
    SlidingFeatureViewITTest,
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
from feathub.feature_views.transforms.tests.test_sliding_window_transform import (
    SlidingWindowTransformITTest,
    SlidingWindowTestConfig,
    ENABLE_EMPTY_WINDOW_OUTPUT_SKIP_SAME_WINDOW_OUTPUT,
)
from feathub.processors.local.local_processor import (
    _is_spark_supported_source,
    _is_spark_supported_sink,
)
from feathub.processors.spark.tests.test_spark_processor import SparkProcessorITTest
from feathub.tests.test_get_features import GetFeaturesITTest
from feathub.tests.test_online_features import OnlineFeaturesITTest


class LocalProcessorTest(unittest.TestCase):
    def test_spark_supported_source_sink(self):
        supported_source_names = set()
        supported_sink_names = set()
        for base_class in SparkProcessorITTest.__bases__:
            base_name = base_class.__name__
            source_sink_match = re.search("(Source|Sink)", base_name)
            if source_sink_match is None:
                continue
            prefix = base_name[: source_sink_match.start()]
            if "Source" in base_name:
                supported_source_names.add(prefix + "Source")
            if "Sink" in base_name:
                supported_sink_names.add(prefix + "Sink")

        for origin_table_class in FeatureTable.__subclasses__():
            table_class: Type[FeatureTable] = origin_table_class  # type: ignore
            if "Source" in table_class.__name__:
                if table_class.__name__ in supported_source_names:
                    self.assertTrue(_is_spark_supported_source(Mock(spec=table_class)))
                else:
                    self.assertFalse(_is_spark_supported_source(Mock(spec=table_class)))
            if "Sink" in table_class.__name__:
                if table_class.__name__ in supported_sink_names:
                    self.assertTrue(_is_spark_supported_sink(Mock(spec=table_class)))
                else:
                    self.assertFalse(_is_spark_supported_sink(Mock(spec=table_class)))


class LocalProcessorITTest(
    BlackHoleSinkITTest,
    DataGenSourceITTest,
    ExpressionTransformITTest,
    FileSystemSourceSinkITTest,
    GetFeaturesITTest,
    JoinTransformITTest,
    OnlineFeaturesITTest,
    OverWindowTransformITTest,
    PrintSinkITTest,
    PythonUDFTransformITTest,
    SlidingWindowTransformITTest,
    DerivedFeatureViewITTest,
    SlidingFeatureViewITTest,
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

    def get_supported_sliding_window_config(self) -> List[SlidingWindowTestConfig]:
        return [
            ENABLE_EMPTY_WINDOW_OUTPUT_SKIP_SAME_WINDOW_OUTPUT,
        ]

    def get_client(self, extra_config: Optional[Dict] = None) -> FeathubClient:
        return self.get_client_with_local_registry(
            {
                "type": "local",
            },
            extra_config,
        )

    # TODO: Enable this test after local processor support datagen source.
    def test_bounded_left_table_join_unbounded_right_table(self):
        pass

    # TODO: Support protobuf format for LocalProcessor FileSystem connector.
    def test_local_file_system_protobuf_source_sink(self):
        pass

    def test_protobuf_all_types(self):
        pass
