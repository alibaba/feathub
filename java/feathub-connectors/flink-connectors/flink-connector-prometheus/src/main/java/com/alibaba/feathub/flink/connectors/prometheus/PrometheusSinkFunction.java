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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.util.StringUtils;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.PushGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static com.alibaba.feathub.flink.connectors.prometheus.PrometheusConfigs.DELETE_ON_SHUTDOWN;
import static com.alibaba.feathub.flink.connectors.prometheus.PrometheusConfigs.EXTRA_LABELS;
import static com.alibaba.feathub.flink.connectors.prometheus.PrometheusConfigs.JOB_NAME;
import static com.alibaba.feathub.flink.connectors.prometheus.PrometheusConfigs.RETRY_TIMEOUT_MS;
import static com.alibaba.feathub.flink.connectors.prometheus.PrometheusConfigs.SERVER_URL;

/**
 * A {@link org.apache.flink.streaming.api.functions.sink.SinkFunction} that writes data to a
 * Prometheus PushGateway. Users can specify lists of labels for each column via the column
 * comments. Label lists are separated by ';', labels within a list are separated by ',', and label
 * names and values are separated by '='. For example: k1=v11,k2=v21;k1=v12,k2=v22. Use backslash
 * '\' to escape '=', ',', and ';' if they are used in label value.
 *
 * <p>The behavior of the Prometheus Sink for different column types is as follows:
 *
 * <ul>
 *   <li>Numeric Column: The value type of this column must be a number. Exactly one metric is
 *       emitted with the metric name equal to the column's name, metric values equal to the
 *       column's values, and metric labels derived from the column's comment as specified above.
 *   <li>Map Column: The value type of this column must be a map that maps from string to a number.
 *       For each key in this map, a metric is emitted with the metric name equal to the column's
 *       name, metric values equal to the values of the given key. The metric labels are derived
 *       from the column's comment as specified above, except that any label whose value is "null"
 *       in the column's comment should be updated to use the given key as the label value.
 *   <li>List Column: The value type of this column must be an array of numbers or an array of maps
 *       that map from strings to numbers. For each position in this array, metrics are emitted with
 *       the metric name equal to the column's name, metric values equal to the element values in
 *       the given position of the array values, and metric labels derived from the label list in
 *       the given position of the column's comment.
 * </ul>
 *
 * <p>Below is an example illustrating how to define a Prometheus Sink table:
 *
 * <pre>
 *     TableDescriptor.forConnector("prometheus")
 *         .schema(
 *             Schema.newBuilder()
 *                 .column(
 *                     "metric_map",
 *                     DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
 *                 .withComment("feature=a,count_map_key=null")
 *                 .column(
 *                     "metric_array",
 *                     DataTypes.ARRAY(DataTypes.INT()))
 *                 .withComment("feature=a,type=int;feature=b,type=int")
 *                 .column(
 *                     "metric_map_array",
 *                     DataTypes.ARRAY(
 *                             DataTypes.MAP(
 *                                 DataTypes.STRING(),
 *                                 DataTypes.DOUBLE())))
 *                 .withComment(
 *                     "feature=null,range=last10m;feature=null,range=last20m")
 *                 .column("metric", DataTypes.DOUBLE())
 *                 .withComment("type=double")
 *                 .build())
 *         .option("serverUrl", "localhost:9091")
 *         .option("jobName", "exampleJob")
 *         .option("extraLabels", "k1=v1,k2=v2")
 *         .build();
 * </pre>
 */
public class PrometheusSinkFunction extends RichSinkFunction<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(PrometheusSinkFunction.class);

    private static final long RETRY_INTERVAL_MS = 1000; // 1 second.

    private final String jobName;
    private final Map<String, String> extraLabels;
    private final List<ColumnInfo> columnInfos;
    private final String serverUrl;
    private final Boolean deleteOnShutdown;
    private final long retryTimeoutMs;
    private CollectorRegistry registry;
    private PushGateway pushGateway;
    private List<MetricUpdater<?>> metricUpdaters;

    public PrometheusSinkFunction(ReadableConfig config, ResolvedSchema schema) {
        this.jobName = config.get(JOB_NAME);
        this.extraLabels = parseLabels(config.get(EXTRA_LABELS));
        this.serverUrl = config.get(SERVER_URL);
        this.deleteOnShutdown = config.get(DELETE_ON_SHUTDOWN);
        this.retryTimeoutMs = config.get(RETRY_TIMEOUT_MS);

        this.columnInfos = new ArrayList<>();
        for (Column column : schema.getColumns()) {
            this.columnInfos.add(ColumnInfo.fromColumn(column));
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        registry = new CollectorRegistry();
        pushGateway = new PushGateway(serverUrl);

        metricUpdaters = new ArrayList<>();
        for (final ColumnInfo columnInfo : this.columnInfos) {
            metricUpdaters.add(columnInfo.getMetricWrapper(registry));
        }
    }

    @Override
    public void invoke(RowData row, Context context) throws Exception {
        for (int i = 0; i < columnInfos.size(); i++) {
            updateMetric(row, i);
        }

        long startTimeMs = System.currentTimeMillis();
        while (true) {
            try {
                pushGateway.pushAdd(registry, jobName, extraLabels);
                break;
            } catch (Exception e) {
                LOG.warn(
                        "Failed to push metrics to PushGateway with jobName {}, groupingKey {}.",
                        jobName,
                        extraLabels,
                        e);
                if (System.currentTimeMillis() - startTimeMs > retryTimeoutMs) {
                    break;
                }
                Thread.sleep(RETRY_INTERVAL_MS);
            }
        }
    }

    public void updateMetric(RowData row, int index) throws IOException {
        final LogicalType type = columnInfos.get(index).dataType.getLogicalType();
        switch (type.getTypeRoot()) {
            case DECIMAL:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                ((SimpleMetricUpdater) metricUpdaters.get(index))
                        .update(getDoubleValue(row, index));
                break;
            case MAP:
                final MapData mapData = row.getMap(index);
                final ArrayData keyArray = mapData.keyArray();
                final ArrayData valueArray = mapData.valueArray();
                Map<String, Double> mapMetric = new HashMap<>();
                for (int j = 0; j < keyArray.size(); j++) {
                    final String keyLabel = keyArray.getString(j).toString();
                    mapMetric.put(
                            keyLabel,
                            getDoubleValue(valueArray, j, ((MapType) type).getValueType()));
                }
                ((MapMetricUpdater) metricUpdaters.get(index)).update(mapMetric);
                break;
            case ARRAY:
                final ArrayData arrayData = row.getArray(index);
                final LogicalType elementType = ((ArrayType) type).getElementType();
                if (elementType.is(LogicalTypeFamily.NUMERIC)) {
                    updateNumberArrayMetrics(index, (ArrayType) type, arrayData);
                } else if (elementType.is(LogicalTypeRoot.MAP)) {
                    updateMapArrayMetrics(index, (ArrayType) type, arrayData);
                } else {
                    throw new RuntimeException(
                            String.format("Unsupported element type: %s in array.", elementType));
                }
                break;
            default:
                throw new IOException(String.format("Unsupported data type: %s", type));
        }
    }

    private void updateMapArrayMetrics(int index, ArrayType type, ArrayData arrayData)
            throws IOException {
        final MapType elementType = (MapType) type.getElementType();
        List<Map<String, Double>> maps = new ArrayList<>();
        for (int j = 0; j < arrayData.size(); ++j) {
            Map<String, Double> map = new HashMap<>();
            final MapData mapData = arrayData.getMap(j);
            for (int k = 0; k < mapData.size(); ++k) {
                map.put(
                        mapData.keyArray().getString(k).toString(),
                        getDoubleValue(mapData.valueArray(), k, elementType.getValueType()));
            }
            maps.add(map);
        }
        ((MapListMetricUpdater) metricUpdaters.get(index)).update(maps);
    }

    private void updateNumberArrayMetrics(int index, ArrayType type, ArrayData arrayData)
            throws IOException {
        List<Double> listMetric = new ArrayList<>();
        for (int j = 0; j < arrayData.size(); ++j) {
            listMetric.add(getDoubleValue(arrayData, j, type.getElementType()));
        }
        ((DoubleListMetricUpdater) metricUpdaters.get(index)).update(listMetric);
    }

    private double getDoubleValue(RowData row, int index) throws IOException {
        final LogicalType type = columnInfos.get(index).dataType.getLogicalType();
        switch (type.getTypeRoot()) {
            case DECIMAL:
                final int precision = ((DecimalType) type).getPrecision();
                final int scale = ((DecimalType) type).getScale();
                final DecimalData decimal = row.getDecimal(index, precision, scale);
                return decimal.toBigDecimal().doubleValue();
            case TINYINT:
                return row.getByte(index);
            case SMALLINT:
                return row.getShort(index);
            case INTEGER:
                return row.getInt(index);
            case BIGINT:
                return row.getLong(index);
            case FLOAT:
                return row.getFloat(index);
            case DOUBLE:
                return row.getDouble(index);
            default:
                throw new IOException(String.format("Unsupported data type: %s", type));
        }
    }

    private double getDoubleValue(ArrayData array, int index, LogicalType type) throws IOException {
        switch (type.getTypeRoot()) {
            case DECIMAL:
                final int precision = ((DecimalType) type).getPrecision();
                final int scale = ((DecimalType) type).getScale();
                final DecimalData decimal = array.getDecimal(index, precision, scale);
                return decimal.toBigDecimal().doubleValue();
            case TINYINT:
                return array.getByte(index);
            case SMALLINT:
                return array.getShort(index);
            case INTEGER:
                return array.getInt(index);
            case BIGINT:
                return array.getLong(index);
            case FLOAT:
                return array.getFloat(index);
            case DOUBLE:
                return array.getDouble(index);
            default:
                throw new IOException(String.format("Unsupported data type: %s", type));
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (deleteOnShutdown) {
            try {
                pushGateway.delete(jobName, extraLabels);
            } catch (IOException e) {
                LOG.warn(
                        "Failed to delete metrics from PushGateway with jobName {}, groupingKey {}.",
                        jobName,
                        extraLabels,
                        e);
            }
        }
    }

    static List<Map<String, String>> parseLabelLists(String labelListsConfig) {
        List<Map<String, String>> labels = new ArrayList<>();
        final String[] labelStrings = split(labelListsConfig, ";");
        for (String labelString : labelStrings) {
            if (labelString.isEmpty()) {
                continue;
            }
            labels.add(parseLabels(labelString));
        }

        return labels;
    }

    static Map<String, String> parseLabels(final String labelsConfig) {
        Map<String, String> labelNames = new LinkedHashMap<>();
        String[] kvs = split(labelsConfig, ",");
        for (String kv : kvs) {
            if (kv.isEmpty()) {
                continue;
            }

            String labelName;
            String labelValue;
            final String[] splitKv = split(kv, "=");
            if (splitKv.length == 2) {
                labelName = splitKv[0];
                labelValue = splitKv[1];
            } else if (splitKv.length == 1) {
                labelName = splitKv[0];
                labelValue = "";
            } else {
                throw new RuntimeException(
                        String.format("Invalid prometheusPushGateway labels:%s", kv));
            }

            if (StringUtils.isNullOrWhitespaceOnly(labelName)) {
                throw new RuntimeException(
                        String.format("Invalid labelName:%s must not be empty", labelName));
            }
            labelNames.put(labelName, labelValue);
        }

        return labelNames;
    }

    private static String[] split(String str, String delim) {
        String regex = "(?<!\\\\)" + Pattern.quote(delim);
        String escaped = "\\" + delim;
        return Arrays.stream(str.split(regex))
                .map(s -> s.replace(escaped, delim))
                .toArray(String[]::new);
    }

    private static class ColumnInfo implements Serializable {
        private final String name;
        private final String comment;
        private final DataType dataType;

        private ColumnInfo(String name, String comment, DataType dataType) {
            this.name = name;
            this.comment = comment;
            this.dataType = dataType;
        }

        private static ColumnInfo fromColumn(Column column) {
            verifyType(column.getDataType().getLogicalType());
            return new ColumnInfo(
                    column.getName(), column.getComment().orElse(""), column.getDataType());
        }

        private static void verifyType(LogicalType logicalType) {
            if (logicalType.is(LogicalTypeRoot.MAP)) {
                final MapType mapType = (MapType) logicalType;
                if (mapType.getKeyType().is(LogicalTypeRoot.VARCHAR)
                        && mapType.getValueType().is(LogicalTypeFamily.NUMERIC)) {
                    return;
                }
            } else if (logicalType.is(LogicalTypeRoot.ARRAY)) {
                final LogicalType elementType = ((ArrayType) logicalType).getElementType();
                if (elementType.is(LogicalTypeRoot.MAP)) {
                    final MapType mapType = (MapType) elementType;
                    if (mapType.getKeyType().is(LogicalTypeRoot.VARCHAR)
                            && mapType.getValueType().is(LogicalTypeFamily.NUMERIC)) {
                        return;
                    }
                } else if (elementType.is(LogicalTypeFamily.NUMERIC)) {
                    return;
                }
            } else if (logicalType.is(LogicalTypeFamily.NUMERIC)) {
                return;
            }
            throw new RuntimeException(
                    "PrometheusSink only support numeric data type, map data type from string type to "
                            + "numeric type, and array data type of numeric type or map data type from "
                            + "string type to numeric type.");
        }

        private MetricUpdater<?> getMetricWrapper(CollectorRegistry registry) {
            final List<Map<String, String>> labels = parseLabelLists(comment);
            switch (dataType.getLogicalType().getTypeRoot()) {
                case DECIMAL:
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                case FLOAT:
                case DOUBLE:
                    if (labels.size() > 1) {
                        throw new RuntimeException(
                                "There can be at most one set of labels for numeric type metric.");
                    }
                    return new SimpleMetricUpdater(
                            name,
                            labels.isEmpty() ? Collections.emptyMap() : labels.get(0),
                            registry);
                case MAP:
                    if (labels.size() != 1) {
                        throw new RuntimeException(
                                "There can be exactly one set of labels for map type metric.");
                    }
                    return new MapMetricUpdater(name, labels.get(0), registry);
                case ARRAY:
                    final LogicalType elementType =
                            ((ArrayType) dataType.getLogicalType()).getElementType();
                    if (elementType.is(LogicalTypeFamily.NUMERIC)) {
                        return new DoubleListMetricUpdater(name, labels, registry);
                    } else if (elementType.is(LogicalTypeRoot.MAP)) {
                        return new MapListMetricUpdater(name, labels, registry);
                    } else {
                        throw new RuntimeException(
                                String.format(
                                        "Unsupported element type: %s in array.", elementType));
                    }
                default:
                    throw new RuntimeException(
                            String.format("Unsupported data type: %s", dataType));
            }
        }
    }
}
