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
from typing import Optional, Dict, List

from feathub.feathub_client import FeathubClient
from feathub.feature_tables.tests.test_file_system_source_sink import (
    FileSystemSourceSinkITTest,
)
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
from feathub.tests.test_get_features import GetFeaturesITTest
from feathub.tests.test_online_features import OnlineFeaturesITTest


class LocalProcessorITTest(
    ExpressionTransformITTest,
    FileSystemSourceSinkITTest,
    GetFeaturesITTest,
    JoinTransformITTest,
    OnlineFeaturesITTest,
    OverWindowTransformITTest,
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
