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
import os
import shutil
import tempfile
import unittest
from typing import Optional, Dict
from unittest.mock import Mock, patch

import pandas as pd
from pyflink import java_gateway
from pyflink.table import Table, TableSchema

from feathub.common.exceptions import (
    FeathubException,
)
from feathub.common.types import Int64, String
from feathub.feathub_client import FeathubClient
from feathub.feature_tables.sinks.file_system_sink import FileSystemSink
from feathub.feature_tables.sinks.memory_store_sink import MemoryStoreSink
from feathub.feature_tables.sources.file_system_source import FileSystemSource
from feathub.feature_tables.tests.test_black_hole_sink import BlackHoleSinkITTest
from feathub.feature_tables.tests.test_datagen_source import DataGenSourceITTest
from feathub.feature_tables.tests.test_file_system_source_sink import (
    FileSystemSourceSinkITTest,
)
from feathub.feature_tables.tests.test_kafka_source_sink import (
    KafkaSourceSinkITTest,
)
from feathub.feature_tables.tests.test_mysql_source_sink import MySQLSourceSinkITTest
from feathub.feature_tables.tests.test_print_sink import PrintSinkITTest
from feathub.feature_tables.tests.test_redis_source_sink import (
    RedisSourceSinkStandaloneModeITTest,
    RedisSourceSinkClusterModeITTest,
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
)
from feathub.metric_stores.tests.test_prometheus_metric_store import (
    PrometheusMetricStoreITTest,
)
from feathub.online_stores.memory_online_store import MemoryOnlineStore
from feathub.processors.flink import flink_table
from feathub.processors.flink.flink_class_loader_utils import (
    get_flink_context_class_loader,
)
from feathub.processors.flink.flink_deployment_mode import DeploymentMode
from feathub.processors.flink.flink_processor import FlinkProcessor

from feathub.processors.flink.table_builder.flink_table_builder import FlinkTableBuilder
from feathub.processors.flink.table_builder.tests.test_flink_sql_feature_view import (
    FlinkSqlFeatureViewITTest,
)
from feathub.processors.materialization_descriptor import (
    MaterializationDescriptor,
)
from feathub.registries.local_registry import LocalRegistry
from feathub.table.schema import Schema
from feathub.tests.test_get_features import GetFeaturesITTest
from feathub.tests.test_materialize_features import MaterializeFeaturesITTest


# TODO: move this file to python/feathub/processors/flink/tests folder after
#  the resource leak problem of other flink processor's tests is fixed.


class FlinkProcessorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.registry = LocalRegistry(props={})

    def tearDown(self) -> None:
        # TODO: replace the cleanup below with flink configuration after
        #  pyflink dependency upgrades to 1.16.0 or higher. Related
        #  ticket: FLINK-27297
        with java_gateway._lock:
            if java_gateway._gateway is not None:
                java_gateway._gateway.shutdown()
                java_gateway._gateway = None
        MemoryOnlineStore.get_instance().reset()

    @classmethod
    def tearDownClass(cls) -> None:
        if "PYFLINK_GATEWAY_DISABLED" in os.environ:
            os.environ.pop("PYFLINK_GATEWAY_DISABLED")

    def test_default_deployment_mode(self):
        processor = FlinkProcessor(
            props={
                "processor.flink.master": "127.0.0.1:1234",
            },
            registry=self.registry,
        )
        self.assertEqual(DeploymentMode.SESSION, processor.deployment_mode)

    def test_unsupported_deployment_mode(self):
        with self.assertRaises(FeathubException):
            FlinkProcessor(
                props={
                    "processor.flink.master": "127.0.0.1:1234",
                    "processor.flink.deployment_mode": "unsupported",
                },
                registry=self.registry,
            )

    def test_session_mode_without_session_cluster_settings(self):
        with self.assertRaises(FeathubException):
            FlinkProcessor(props={}, registry=self.registry)

    def test_get_table_with_session_mode(self):
        processor = FlinkProcessor(
            props={
                "processor.flink.master": "127.0.0.1:1234",
            },
            registry=self.registry,
        )

        mock_table_builder = Mock(spec=FlinkTableBuilder)
        processor.flink_table_builder = mock_table_builder
        source = FileSystemSource("source", "/path", "csv", Schema([], []))
        table = processor.get_table(source)
        self.assertEqual(source, table.feature)

    def test_flink_processor_with_session_mode(self):
        processor = FlinkProcessor(
            props={
                "processor.flink.master": "127.0.0.1:1234",
            },
            registry=self.registry,
        )
        configuration = dict(
            processor.flink_table_builder.t_env._get_j_env().getConfiguration().toMap()
        )
        self.assertEqual(configuration.get("rest.address"), "127.0.0.1")
        self.assertEqual(configuration.get("rest.port"), "1234")

    def test_materialize_with_session_mode(self):
        processor = FlinkProcessor(
            props={
                "processor.flink.master": "127.0.0.1:1234",
            },
            registry=self.registry,
        )
        mock_table = Mock(spec=Table)
        mock_schema = Mock(spec=TableSchema)
        mock_schema.get_field_names.return_value = []
        mock_table.get_schema.return_value = mock_schema

        mock_table_builder = Mock(spec=FlinkTableBuilder)
        mock_table_builder.build.return_value = mock_table
        mock_t_env = Mock()
        mock_statement_set = Mock()
        mock_t_env.create_statement_set.return_value = mock_statement_set
        mock_table_builder.t_env = mock_t_env
        mock_table_builder.class_loader = get_flink_context_class_loader()
        processor.flink_table_builder = mock_table_builder
        source = FileSystemSource("source", "/path", "csv", Schema([], []))
        sink = FileSystemSink("/path", "csv")

        processor.materialize_features(
            materialization_descriptors=[
                MaterializationDescriptor(
                    feature_descriptor=source, sink=sink, allow_overwrite=True
                )
            ]
        )
        mock_table_builder.build.assert_called_once()
        self.assertEqual(source, mock_table_builder.build.call_args[1]["features"])
        mock_statement_set.add_insert.assert_called_once()

    def test_materialize_to_online_store_with_session_mode(self):
        processor = FlinkProcessor(
            props={
                "processor.flink.master": "127.0.0.1:1234",
            },
            registry=self.registry,
        )
        expected_df = pd.DataFrame(
            [
                [1, 4, "2022-01-01 00:00:00"],
                [2, 5, "2022-01-01 00:00:00"],
                [3, 6, "2022-01-01 00:00:00"],
                [1, 7, "2022-01-01 00:02:00"],
            ],
            columns=["key", "val", "time"],
        )
        mock_table: Table = Mock(spec=Table)
        mock_table_builder = Mock(spec=FlinkTableBuilder)
        mock_table_builder.class_loader = get_flink_context_class_loader()
        mock_table_builder.build.return_value = mock_table
        processor.flink_table_builder = mock_table_builder
        source = FileSystemSource(
            "source",
            "/path",
            "csv",
            Schema(["key", "val", "time"], [Int64, Int64, String]),
            keys=["key"],
            timestamp_field="time",
            timestamp_format="%Y-%m-%d %H:%M:%S",
        )
        sink = MemoryStoreSink("test_table")

        with patch.object(flink_table, "flink_table_to_pandas") as to_pandas_method:
            to_pandas_method.return_value = expected_df
            processor.materialize_features(
                materialization_descriptors=[
                    MaterializationDescriptor(
                        feature_descriptor=source, sink=sink, allow_overwrite=True
                    )
                ]
            )

            self.assertTrue(
                pd.Series([7]).equals(
                    MemoryOnlineStore.get_instance().get(
                        "test_table", pd.DataFrame([[1]], columns=["key"])
                    )["val"]
                )
            )

    def test_flink_config(self):
        processor = FlinkProcessor(
            props={
                "processor.flink.deployment_mode": "cli",
                "processor.flink.native.key": "value",
            },
            registry=self.registry,
        )

        self.assertEqual(
            processor.flink_table_builder.t_env.get_config().get("key", None), "value"
        )


class FlinkProcessorITTest(
    DataGenSourceITTest,
    DerivedFeatureViewITTest,
    ExpressionTransformITTest,
    FileSystemSourceSinkITTest,
    JoinTransformITTest,
    KafkaSourceSinkITTest,
    OverWindowTransformITTest,
    PrintSinkITTest,
    PrometheusMetricStoreITTest,
    PythonUDFTransformITTest,
    RedisSourceSinkStandaloneModeITTest,
    RedisSourceSinkClusterModeITTest,
    SlidingWindowTransformITTest,
    SlidingFeatureViewITTest,
    BlackHoleSinkITTest,
    FlinkSqlFeatureViewITTest,
    GetFeaturesITTest,
    MySQLSourceSinkITTest,
    MaterializeFeaturesITTest,
):
    __test__ = True

    _cached_clients: Dict[str, FeathubClient] = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.invoke_all_base_class_setupclass()

        # Due to the resource leak in PyFlink StreamExecutionEnvironment and
        # StreamTableEnvironment https://issues.apache.org/jira/browse/FLINK-30258.
        # We want to share env and t_env across all the tests in one class to mitigate
        # the leak.
        # TODO: After the ticket is resolved, we should clean up the resource in
        #  StreamExecutionEnvironment and StreamTableEnvironment after every test to
        #  fully avoid resource leak.
        cls._cached_clients = {}

    def setUp(self):
        self.invoke_base_class_setup()
        self.temp_dir = tempfile.mkdtemp()
        self.input_data, self.schema = self.create_input_data_and_schema()

    def tearDown(self) -> None:
        self.invoke_base_class_teardown()
        MemoryOnlineStore.get_instance().reset()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.invoke_all_base_class_teardownclass()
        if "PYFLINK_GATEWAY_DISABLED" in os.environ:
            os.environ.pop("PYFLINK_GATEWAY_DISABLED")

    def get_client(self, extra_config: Optional[Dict] = None) -> FeathubClient:
        if str(extra_config) not in self._cached_clients:
            self._cached_clients[
                str(extra_config)
            ] = self.get_client_with_local_registry(
                {
                    "type": "flink",
                    "flink": {
                        "deployment_mode": "cli",
                    },
                },
                extra_config,
            )
        return self._cached_clients[str(extra_config)]

    def test_unsupported_file_format(self):
        source = self.create_file_source(self.input_data)
        sink = FileSystemSink("s3://dummy-bucket/path", "csv")
        with self.assertRaisesRegex(
            FeathubException, "Cannot sink files in CSV format to s3"
        ):
            self.client.materialize_features(
                feature_descriptor=source, sink=sink, allow_overwrite=True
            )

    def test_random_field_max_past(self):
        pass

    def test_random_field_length(self):
        pass
