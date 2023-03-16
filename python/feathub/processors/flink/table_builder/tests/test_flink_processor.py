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
import glob
import os
import shutil
import tempfile
import unittest
from typing import Optional, Dict
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
from pyflink import java_gateway
from pyflink.table import Table, TableSchema

from feathub.common.exceptions import (
    FeathubException,
)
from feathub.common.types import Int32, Int64, String
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
from feathub.feature_tables.tests.test_print_sink import PrintSinkITTest
from feathub.feature_tables.tests.test_redis_source_sink import (
    RedisSourceSinkITTest,
)
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature import Feature
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
from feathub.online_stores.memory_online_store import MemoryOnlineStore
from feathub.processors.flink import flink_table
from feathub.processors.flink.flink_deployment_mode import DeploymentMode
from feathub.processors.flink.flink_processor import FlinkProcessor
from feathub.processors.flink.job_submitter.flink_job_submitter import (
    FlinkJobSubmitter,
)
from feathub.processors.flink.job_submitter.flink_kubernetes_application_cluster_job_submitter import (  # noqa
    FlinkKubernetesApplicationClusterJobSubmitter,
)
from feathub.processors.flink.table_builder.flink_table_builder import FlinkTableBuilder
from feathub.registries.local_registry import LocalRegistry
from feathub.table.schema import Schema


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
                "processor.flink.rest.address": "127.0.0.1",
                "processor.flink.rest.port": 1234,
            },
            registry=self.registry,
        )
        self.assertEqual(DeploymentMode.SESSION, processor.deployment_mode)

    def test_unsupported_deployment_mode(self):
        with self.assertRaises(FeathubException):
            FlinkProcessor(
                props={
                    "processor.flink.rest.address": "127.0.0.1",
                    "processor.flink.rest.port": 1234,
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
                "processor.flink.rest.address": "127.0.0.1",
                "processor.flink.rest.port": 1234,
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
                "processor.flink.rest.address": "127.0.0.1",
                "processor.flink.rest.port": 1234,
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
                "processor.flink.rest.address": "127.0.0.1",
                "processor.flink.rest.port": 1234,
            },
            registry=self.registry,
        )
        mock_table = Mock(spec=Table)
        mock_schema = Mock(spec=TableSchema)
        mock_schema.get_field_names.return_value = []
        mock_table.get_schema.return_value = mock_schema

        mock_table_builder = Mock(spec=FlinkTableBuilder)
        mock_table_builder.build.return_value = mock_table
        mock_table_builder.t_env = Mock()
        processor.flink_table_builder = mock_table_builder
        source = FileSystemSource("source", "/path", "csv", Schema([], []))
        sink = FileSystemSink("/path", "csv")

        processor.materialize_features(source, sink, allow_overwrite=True)
        mock_table_builder.build.assert_called_once()
        self.assertEqual(source, mock_table_builder.build.call_args[1]["features"])
        mock_table.execute_insert.assert_called_once()

    def test_to_pandas_with_kubernetes_application_mode(self):
        processor = FlinkProcessor(
            props={
                "flink_home": "/flink/home",
                "processor.flink.deployment_mode": "kubernetes-application",
            },
            registry=self.registry,
        )
        self.assertTrue(
            isinstance(
                processor.flink_job_submitter,
                FlinkKubernetesApplicationClusterJobSubmitter,
            )
        )
        table = processor.get_table(
            FileSystemSource("source", "path", "csv", Schema([], []))
        )
        with self.assertRaises(FeathubException):
            table.to_pandas()

    def test_table_schema_with_kubernetes_application_mode(self):
        processor = FlinkProcessor(
            props={
                "flink_home": "/flink/home",
                "processor.flink.deployment_mode": "kubernetes-application",
            },
            registry=self.registry,
        )
        self.assertTrue(
            isinstance(
                processor.flink_job_submitter,
                FlinkKubernetesApplicationClusterJobSubmitter,
            )
        )
        schema = Schema(["id"], [Int32])
        table = processor.get_table(FileSystemSource("source", "path", "csv", schema))
        self.assertEqual(schema, table.get_schema())

    def test_table_execute_insert_with_kubernetes_application_mode(self):
        processor = FlinkProcessor(
            props={
                "flink_home": "flink/home",
                "processor.flink.deployment_mode": "kubernetes-application",
            },
            registry=self.registry,
        )
        schema = Schema(["id"], [Int32])
        source = FileSystemSource("source", "path", "csv", schema)
        feature_view = DerivedFeatureView(
            "feature_view",
            source=source,
            features=[Feature(name="id", transform="id")],
        )
        self.registry.build_features([feature_view])

        sink = FileSystemSink("/path", "csv")
        mock_submitter = MagicMock(spec=FlinkJobSubmitter)
        processor.flink_job_submitter = mock_submitter
        processor.materialize_features(feature_view, sink, allow_overwrite=True)
        mock_submitter.submit.assert_called_once()

        self.assertEqual(feature_view, mock_submitter.submit.call_args[1]["features"])

    def test_materialize_joined_feature_application_mode(self):
        processor = FlinkProcessor(
            props={
                "flink_home": "flink/home",
                "processor.flink.deployment_mode": "kubernetes-application",
            },
            registry=self.registry,
        )

        dim_source = FileSystemSource(
            "dim_source", "/path", "csv", Schema(["a", "id"], [Int32, Int32])
        )
        dim_feature_view = DerivedFeatureView(
            "dim_feature_view",
            source=dim_source,
            features=[Feature(name="a", transform="a", keys=["id"])],
        )

        dim_source2 = FileSystemSource(
            "dim_source2", "/path", "csv", Schema(["b", "id"], [Int32, Int32])
        )
        dim_feature_view_2 = DerivedFeatureView(
            "dim_feature_view_2",
            source=dim_source2,
            features=[Feature(name="b", transform="b", keys=["id"])],
        )

        source = FileSystemSource("source", "/path", "csv", Schema(["id"], [Int32]))
        joined_feature_view = DerivedFeatureView(
            "joined_feature_view", source, features=["dim_feature_view.a"]
        )

        source_2 = FileSystemSource("source2", "/path", "csv", Schema(["id"], [Int32]))
        joined_feature_view_2 = DerivedFeatureView(
            "joined_feature_view_2", source_2, features=["dim_feature_view_2.b"]
        )

        feature_view = DerivedFeatureView(
            "feature_view",
            source=joined_feature_view,
            features=["joined_feature_view_2.b"],
        )

        self.registry.build_features(
            [
                dim_feature_view,
                dim_feature_view_2,
                joined_feature_view,
                joined_feature_view_2,
                feature_view,
            ]
        )

        sink = FileSystemSink("/path", "csv")
        mock_submitter = MagicMock(spec=FlinkJobSubmitter)
        processor.flink_job_submitter = mock_submitter
        processor.materialize_features(feature_view, sink, allow_overwrite=True)
        mock_submitter.submit.assert_called_once()

        self.assertEqual(
            {
                "dim_feature_view": self.registry.get_features("dim_feature_view"),
                "joined_feature_view_2": self.registry.get_features(
                    "joined_feature_view_2"
                ),
                "dim_feature_view_2": self.registry.get_features("dim_feature_view_2"),
            },
            mock_submitter.submit.call_args[1]["local_registry_tables"],
        )

    def test_materialize_to_online_store_with_session_mode(self):
        processor = FlinkProcessor(
            props={
                "processor.flink.rest.address": "127.0.0.1",
                "processor.flink.rest.port": 1234,
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
            processor.materialize_features(source, sink, allow_overwrite=True)

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
    PythonUDFTransformITTest,
    RedisSourceSinkITTest,
    SlidingWindowTransformITTest,
    SlidingFeatureViewITTest,
    BlackHoleSinkITTest,
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

    def test_read_write(self):
        source = self.create_file_source(self.input_data)

        sink_path = tempfile.NamedTemporaryFile(dir=self.temp_dir).name

        sink = FileSystemSink(sink_path, "csv")

        self.client.materialize_features(
            features=source,
            sink=sink,
            allow_overwrite=True,
        ).wait()

        files = glob.glob(f"{sink_path}/*")
        df = pd.DataFrame()
        for f in files:
            csv = pd.read_csv(f, names=["name", "cost", "distance", "time"])
            df = df.append(csv)
        df = df.sort_values(by=["time"]).reset_index(drop=True)
        self.assertTrue(self.input_data.equals(df))

    def test_unsupported_file_format(self):
        source = self.create_file_source(self.input_data)
        sink = FileSystemSink("s3://dummy-bucket/path", "csv")
        with self.assertRaisesRegex(
            FeathubException, "Cannot sink files in CSV format to s3"
        ):
            self.client.materialize_features(
                features=source, sink=sink, allow_overwrite=True
            )

    # TODO: Fix the bug that FlinkProcessor to_pandas does not support none values.
    def test_join_sliding_feature(self):
        pass

    # TODO: Fix the bug that FlinkProcessor to_pandas does not support none values.
    def test_over_window_on_join_field(self):
        pass

    def test_over_window_transform_first_last_value_with_window_size(self):
        pass

    def test_over_window_transform_first_last_value_with_limit(self):
        pass

    def test_random_field_max_past(self):
        pass

    def test_random_field_length(self):
        pass
