/*
 * Copyright 2022 The FeatHub Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.feathub.flink.connectors.prometheus;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests Prometheus sink. */
public class PrometheusSinkITTest extends PrometheusTableTestBase {

    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;

    @BeforeEach
    void setUp() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
    }

    @Test
    void testSink() throws ExecutionException, InterruptedException, IOException {
        final Table sourceTable =
                tEnv.fromDataStream(env.fromElements(Row.of(0), Row.of(99))).as("metric_val");

        tEnv.createTemporaryView("sourceTable", sourceTable);

        final Table metricTable =
                tEnv.sqlQuery(
                        "SELECT CAST(metric_val AS TINYINT) AS tinyint_val, "
                                + "CAST(metric_val AS SMALLINT) AS smallint_val, "
                                + "CAST(metric_val AS INTEGER) AS int_val, "
                                + "CAST(metric_val AS BIGINT) AS bigint_val, "
                                + "CAST(metric_val AS DECIMAL) AS decimal_val, "
                                + "CAST(metric_val AS FLOAT) AS float_val, "
                                + "CAST(metric_val AS DOUBLE) AS double_val FROM sourceTable");

        final String serverUrl = PROMETHEUS_PUSH_GATEWAY_CONTAINER.getHostUrl();
        metricTable
                .executeInsert(
                        TableDescriptor.forConnector("prometheus")
                                .option("serverUrl", serverUrl)
                                .option("deleteOnShutdown", "false")
                                .option("jobName", "testSink")
                                .option("extraLabels", "k1=v1,k2=v2")
                                .build())
                .await();

        final HashMap<String, List<Metric>> metrics = getMetrics(serverUrl);
        final HashMap<String, String> expectedLabel = new HashMap<>();
        expectedLabel.put("job", "\"testSink\"");
        expectedLabel.put("instance", "\"\"");
        expectedLabel.put("k1", "\"v1\"");
        expectedLabel.put("k2", "\"v2\"");

        checkMetric(metrics.get("tinyint_val").get(0), "tinyint_val", "99", expectedLabel);
        checkMetric(metrics.get("smallint_val").get(0), "smallint_val", "99", expectedLabel);
        checkMetric(metrics.get("int_val").get(0), "int_val", "99", expectedLabel);
        checkMetric(metrics.get("bigint_val").get(0), "bigint_val", "99", expectedLabel);
        checkMetric(metrics.get("decimal_val").get(0), "decimal_val", "99", expectedLabel);
        checkMetric(metrics.get("float_val").get(0), "float_val", "99", expectedLabel);
        checkMetric(metrics.get("double_val").get(0), "double_val", "99", expectedLabel);
    }

    @Test
    void testSinkColumnWithComment() throws ExecutionException, InterruptedException, IOException {
        final Table sourceTable =
                tEnv.fromDataStream(env.fromElements(Row.of(0), Row.of(99))).as("metric_val");

        tEnv.createTemporaryView("sourceTable", sourceTable);

        final Table metricTable =
                tEnv.sqlQuery(
                        "SELECT CAST(metric_val AS TINYINT) AS tinyint_val, "
                                + "CAST(metric_val AS SMALLINT) AS smallint_val, "
                                + "CAST(metric_val AS INTEGER) AS int_val, "
                                + "CAST(metric_val AS BIGINT) AS bigint_val, "
                                + "CAST(metric_val AS DECIMAL) AS decimal_val, "
                                + "CAST(metric_val AS FLOAT) AS float_val, "
                                + "CAST(metric_val AS DOUBLE) AS double_val FROM sourceTable");

        final String serverUrl = PROMETHEUS_PUSH_GATEWAY_CONTAINER.getHostUrl();
        metricTable
                .executeInsert(
                        TableDescriptor.forConnector("prometheus")
                                .schema(
                                        Schema.newBuilder()
                                                .column("tinyint_val", DataTypes.TINYINT())
                                                .withComment(
                                                        "value_type=tinyint,col_k=col_v,test_label=\\;")
                                                .column("smallint_val", DataTypes.SMALLINT())
                                                .withComment("value_type=smallint,col_k=col_v")
                                                .column("int_val", DataTypes.INT())
                                                .withComment("value_type=int,col_k=col_v")
                                                .column("bigint_val", DataTypes.BIGINT())
                                                .withComment("value_type=bigint,col_k=col_v")
                                                .column("decimal_val", DataTypes.DECIMAL(10, 2))
                                                .withComment("value_type=decimal,col_k=col_v")
                                                .column("float_val", DataTypes.FLOAT())
                                                .withComment("value_type=float,col_k=col_v")
                                                .column("double_val", DataTypes.DOUBLE())
                                                .withComment("value_type=double,col_k=col_v")
                                                .build())
                                .option("serverUrl", serverUrl)
                                .option("deleteOnShutdown", "false")
                                .option("jobName", "testSink")
                                .option("extraLabels", "k1=v1,k2=v2")
                                .build())
                .await();

        final HashMap<String, List<Metric>> metrics = getMetrics(serverUrl);
        final HashMap<String, String> tableLabels = new HashMap<>();
        tableLabels.put("job", "\"testSink\"");
        tableLabels.put("instance", "\"\"");
        tableLabels.put("k1", "\"v1\"");
        tableLabels.put("k2", "\"v2\"");
        tableLabels.put("col_k", "\"col_v\"");

        checkMetric(
                metrics.get("tinyint_val").get(0),
                "tinyint_val",
                "99",
                getExpectedLabels(tableLabels, "value_type", "\"tinyint\"", "test_label", "\";\""));
        checkMetric(
                metrics.get("smallint_val").get(0),
                "smallint_val",
                "99",
                getExpectedLabels(tableLabels, "value_type", "\"smallint\""));
        checkMetric(
                metrics.get("int_val").get(0),
                "int_val",
                "99",
                getExpectedLabels(tableLabels, "value_type", "\"int\""));
        checkMetric(
                metrics.get("bigint_val").get(0),
                "bigint_val",
                "99",
                getExpectedLabels(tableLabels, "value_type", "\"bigint\""));
        checkMetric(
                metrics.get("decimal_val").get(0),
                "decimal_val",
                "99",
                getExpectedLabels(tableLabels, "value_type", "\"decimal\""));
        checkMetric(
                metrics.get("float_val").get(0),
                "float_val",
                "99",
                getExpectedLabels(tableLabels, "value_type", "\"float\""));
        checkMetric(
                metrics.get("double_val").get(0),
                "double_val",
                "99",
                getExpectedLabels(tableLabels, "value_type", "\"double\""));
    }

    @Test
    void testSinkArrayMetric() throws ExecutionException, InterruptedException, IOException {
        Integer[] arrayOfInt = new Integer[2];
        arrayOfInt[0] = 100;
        arrayOfInt[1] = 101;
        Map<String, Double>[] arrayOfMap = new Map[2];
        arrayOfMap[0] = new HashMap<>();
        arrayOfMap[0].put("a", 100d);
        arrayOfMap[0].put("b", 101d);
        arrayOfMap[1] = new HashMap<>();
        arrayOfMap[1].put("a", 101d);
        arrayOfMap[1].put("b", 102d);

        final Row row = new Row(2);
        row.setField(0, arrayOfInt);
        row.setField(1, arrayOfMap);
        final Table metricTable =
                tEnv.fromDataStream(
                                env.fromElements(row),
                                Schema.newBuilder()
                                        .column("f0", DataTypes.ARRAY(DataTypes.INT()))
                                        .column(
                                                "f1",
                                                DataTypes.ARRAY(
                                                        DataTypes.MAP(
                                                                DataTypes.STRING(),
                                                                DataTypes.DOUBLE())))
                                        .build())
                        .as("metric_map");

        final String serverUrl = PROMETHEUS_PUSH_GATEWAY_CONTAINER.getHostUrl();
        metricTable
                .executeInsert(
                        TableDescriptor.forConnector("prometheus")
                                .schema(
                                        Schema.newBuilder()
                                                .column(
                                                        "metric_array",
                                                        DataTypes.ARRAY(DataTypes.INT()))
                                                .withComment("k1=v11,k2=v21;k1=v12,k2=v22")
                                                .column(
                                                        "metricMapArray",
                                                        DataTypes.ARRAY(
                                                                DataTypes.MAP(
                                                                        DataTypes.STRING(),
                                                                        DataTypes.DOUBLE())))
                                                .withComment(
                                                        "feature=null,range=last10m;feature=null,range=last20m")
                                                .build())
                                .option("serverUrl", serverUrl)
                                .option("deleteOnShutdown", "false")
                                .option("jobName", "testSink")
                                .option("extraLabels", "k3=v3")
                                .build())
                .await();

        final HashMap<String, List<Metric>> metrics = getMetrics(serverUrl);

        final List<Metric> metricArray = metrics.get("metric_array");
        assertThat(metricArray).hasSize(2);
        assertThat(metricArray)
                .anyMatch(
                        metric ->
                                metric.getLabels().get("k1").equals("\"v11\"")
                                        && metric.getLabels().get("k2").equals("\"v21\"")
                                        && metric.getLabels().get("k3").equals("\"v3\"")
                                        && metric.getValue().equals("100"));
        assertThat(metricArray)
                .anyMatch(
                        metric ->
                                metric.getLabels().get("k1").equals("\"v12\"")
                                        && metric.getLabels().get("k2").equals("\"v22\"")
                                        && metric.getLabels().get("k3").equals("\"v3\"")
                                        && metric.getValue().equals("101"));

        final List<Metric> metricMapArray = metrics.get("metricMapArray");
        assertThat(metricMapArray).hasSize(4);
        assertThat(metricMapArray)
                .anyMatch(
                        metric ->
                                metric.getLabels().get("range").equals("\"last10m\"")
                                        && metric.getLabels().get("feature").equals("\"a\"")
                                        && metric.getValue().equals("100"));
        assertThat(metricMapArray)
                .anyMatch(
                        metric ->
                                metric.getLabels().get("range").equals("\"last10m\"")
                                        && metric.getLabels().get("feature").equals("\"b\"")
                                        && metric.getValue().equals("101"));
        assertThat(metricMapArray)
                .anyMatch(
                        metric ->
                                metric.getLabels().get("range").equals("\"last20m\"")
                                        && metric.getLabels().get("feature").equals("\"a\"")
                                        && metric.getValue().equals("101"));
        assertThat(metricMapArray)
                .anyMatch(
                        metric ->
                                metric.getLabels().get("range").equals("\"last20m\"")
                                        && metric.getLabels().get("feature").equals("\"b\"")
                                        && metric.getValue().equals("102"));
    }

    @Test
    void testSinkArrayMetricWithInvalidLabels() {
        Integer[] arrayMetric = new Integer[2];
        arrayMetric[0] = 100;
        arrayMetric[1] = 101;
        final Row row = new Row(1);
        row.setField(0, arrayMetric);
        final Table metricTable =
                tEnv.fromDataStream(
                                env.fromElements(row),
                                Schema.newBuilder()
                                        .column("f0", DataTypes.ARRAY(DataTypes.INT()))
                                        .build())
                        .as("metric_map");

        final String serverUrl = PROMETHEUS_PUSH_GATEWAY_CONTAINER.getHostUrl();

        assertThatThrownBy(
                        () -> {
                            metricTable
                                    .executeInsert(
                                            TableDescriptor.forConnector("prometheus")
                                                    .schema(
                                                            Schema.newBuilder()
                                                                    .column(
                                                                            "metric_array",
                                                                            DataTypes.ARRAY(
                                                                                    DataTypes
                                                                                            .INT()))
                                                                    .withComment(
                                                                            "k1=v11,k2=v21;k1=v12,k2=v22,k3=v32")
                                                                    .build())
                                                    .option("serverUrl", serverUrl)
                                                    .option("deleteOnShutdown", "false")
                                                    .option("jobName", "testSink")
                                                    .build())
                                    .await();
                        })
                .rootCause()
                .hasMessageContaining("must have the same set of label keys");
    }

    @Test
    void testSinkMapMetric() throws ExecutionException, InterruptedException, IOException {
        Map<String, Integer> metricMap = new HashMap<>();
        metricMap.put("a", 100);
        metricMap.put("b", 101);
        final Table metricTable =
                tEnv.fromDataStream(
                                env.fromElements(Row.of(metricMap)),
                                Schema.newBuilder()
                                        .column(
                                                "f0",
                                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                                        .build())
                        .as("metric_map");

        final String serverUrl = PROMETHEUS_PUSH_GATEWAY_CONTAINER.getHostUrl();
        metricTable
                .executeInsert(
                        TableDescriptor.forConnector("prometheus")
                                .schema(
                                        Schema.newBuilder()
                                                .column(
                                                        "metric_map",
                                                        DataTypes.MAP(
                                                                DataTypes.STRING(),
                                                                DataTypes.INT()))
                                                .withComment("count_map_key=null")
                                                .build())
                                .option("serverUrl", serverUrl)
                                .option("deleteOnShutdown", "false")
                                .option("jobName", "testSink")
                                .option("extraLabels", "k1=v1,k2=v2")
                                .build())
                .await();

        final HashMap<String, List<Metric>> metrics = getMetrics(serverUrl);

        final List<Metric> mapMetric = metrics.get("metric_map");
        assertThat(mapMetric).hasSize(2);
        assertThat(mapMetric)
                .anyMatch(
                        metric ->
                                metric.getLabels().get("count_map_key").equals("\"a\"")
                                        && metric.getValue().equals("100"));
        assertThat(mapMetric)
                .anyMatch(
                        metric ->
                                metric.getLabels().get("count_map_key").equals("\"b\"")
                                        && metric.getValue().equals("101"));
    }

    private Map<String, String> getExpectedLabels(
            Map<String, String> tableLabels, String... keyValues) {
        final HashMap<String, String> res = new HashMap<>(tableLabels);
        for (int i = 0; i < keyValues.length; i += 2) {
            res.put(keyValues[i], keyValues[i + 1]);
        }
        return res;
    }

    private void checkMetric(
            Metric metric, String name, String value, Map<String, String> expectedLabels) {
        assertThat(metric.getName()).isEqualTo(name);
        assertThat(metric.getValue()).isEqualTo(value);
        assertThat(metric.getLabels()).containsAllEntriesOf(expectedLabels);
    }

    private static HashMap<String, List<Metric>> getMetrics(String serverUrl) throws IOException {
        URL url = new URL("http://" + serverUrl + "/metrics");
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("GET");
        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
        String inputLine;

        final HashMap<String, List<Metric>> metrics = new HashMap<>();
        while ((inputLine = in.readLine()) != null) {
            if (inputLine.startsWith("#")) {
                // Ignore comment line.
                continue;
            }

            final Metric metric = Metric.parseMetric(inputLine);
            metrics.computeIfAbsent(metric.name, key -> new ArrayList<>()).add(metric);
        }
        in.close();

        return metrics;
    }

    private static class Metric {
        private final String name;
        private final String value;
        private final Map<String, String> labels;

        public Metric(String name, String value, Map<String, String> labels) {
            this.name = name;
            this.value = value;
            this.labels = labels;
        }

        public static Metric parseMetric(String metricString) throws IllegalArgumentException {
            final String[] split = metricString.split(" ");

            if (split.length != 2) {
                throw new IllegalArgumentException(
                        String.format("Illegal metric string: %s", metricString));
            }

            final String metricAndLabel = split[0].trim();
            final String value = split[1].trim();
            final Pattern pattern = Pattern.compile("(.*)\\{(.*)\\}");
            final Matcher matcher = pattern.matcher(metricAndLabel);
            if (!matcher.find()) {
                return new Metric(metricAndLabel, value, Collections.emptyMap());
            }
            final String metricName = matcher.group(1);
            final String labelsString = matcher.group(2);
            final HashMap<String, String> labels = new HashMap<>();
            for (String pair : labelsString.split(",")) {
                final String[] splitPair = pair.split("=");
                if (splitPair.length != 2) {
                    throw new IllegalArgumentException(String.format("Illegal label: %s", pair));
                }
                labels.put(splitPair[0], splitPair[1]);
            }
            return new Metric(metricName, split[1], labels);
        }

        public String getName() {
            return name;
        }

        public String getValue() {
            return value;
        }

        public Map<String, String> getLabels() {
            return labels;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Metric metric = (Metric) o;
            return Objects.equals(name, metric.name)
                    && Objects.equals(value, metric.value)
                    && Objects.equals(labels, metric.labels);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, value, labels);
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer("Metric{");
            sb.append("name='").append(name).append('\'');
            sb.append(", value='").append(value).append('\'');
            sb.append(", labels=").append(labels);
            sb.append('}');
            return sb.toString();
        }
    }
}
