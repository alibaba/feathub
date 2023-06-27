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

package com.alibaba.feathub.flink.connectors.redis.lookup;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;

import com.alibaba.feathub.flink.connectors.redis.ConversionUtils;
import com.alibaba.feathub.flink.connectors.redis.JedisClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.feathub.flink.connectors.redis.ConversionUtils.OBJECT_MAPPER;
import static com.alibaba.feathub.flink.connectors.redis.RedisConfigs.HASH_FIELDS;
import static com.alibaba.feathub.flink.connectors.redis.RedisConfigs.KEY_FIELDS;

/**
 * A {@link org.apache.flink.streaming.api.functions.source.SourceFunction} used to query a Redis
 * database in a lookup join.
 *
 * <p>Apart from the columns specified in the configuration keyFields(logical key fields), The input
 * table should contain an even number of columns. half of the columns should have the prefix
 * "__KEY__" in their column names(physical key fields) and, once the prefix is removed, their
 * columns names should be equal to those of the other half(feature fields).
 *
 * <p>Each lookup join should pass in the following key fields in order:
 *
 * <ul>
 *   <li>All logical key fields, whose order should be the same as that in the keyFields
 *       configuration value, and
 *   <li>All physical key fields, whose order should be the same as the order of the corresponding
 *       feature fields in Redis table's schema.
 * </ul>
 *
 * <p>During a lookup join, the logical and physical key fields in the output row would be fulfilled
 * by directly passing the values from input row. The feature fields would be fulfilled by querying
 * against the Redis database, where the keys in the queries comes from the corresponding physical
 * key fields.
 */
public class RedisLookupFunction extends LookupFunction {

    private final ReadableConfig config;

    private final int[] logicalKeyFieldIndices;
    private final int[] physicalKeyFieldIndices;
    private final int[] featureFieldIndices;

    private final DataType[] fieldDataTypes;

    private final String[][] hashFields;

    private transient JedisClient client;

    private transient RowData.FieldGetter[] logicalKeyGetters;

    private transient RowData.FieldGetter[] physicalKeyGetters;

    public RedisLookupFunction(ReadableConfig config, ResolvedSchema schema) {
        this.config = config;
        this.logicalKeyFieldIndices = getLogicalKeyFieldIndices(schema, config);
        this.featureFieldIndices = getFeatureFieldIndices(schema, config);
        this.physicalKeyFieldIndices = getPhysicalKeyFieldIndices(schema, featureFieldIndices);
        this.fieldDataTypes = schema.getColumnDataTypes().toArray(new DataType[0]);
        if (config.get(HASH_FIELDS) == null) {
            this.hashFields = null;
        } else {
            try {
                TypeReference<HashMap<String, List<String>>> typeRef =
                        new TypeReference<HashMap<String, List<String>>>() {};
                HashMap<String, List<String>> map =
                        OBJECT_MAPPER.readValue(config.get(HASH_FIELDS), typeRef);
                this.hashFields = new String[schema.getColumnCount()][];
                for (Map.Entry<String, List<String>> entry : map.entrySet()) {
                    this.hashFields[schema.getColumnNames().indexOf(entry.getKey())] =
                            entry.getValue().toArray(new String[0]);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        this.client = JedisClient.create(this.config);
        this.logicalKeyGetters = new RowData.FieldGetter[logicalKeyFieldIndices.length];
        for (int i = 0; i < logicalKeyFieldIndices.length; i++) {
            logicalKeyGetters[i] =
                    RowData.createFieldGetter(
                            fieldDataTypes[logicalKeyFieldIndices[i]].getLogicalType(), i);
        }
        this.physicalKeyGetters = new RowData.FieldGetter[physicalKeyFieldIndices.length];
        for (int i = 0; i < physicalKeyFieldIndices.length; i++) {
            physicalKeyGetters[i] =
                    RowData.createFieldGetter(
                            fieldDataTypes[physicalKeyFieldIndices[i]].getLogicalType(),
                            i + logicalKeyFieldIndices.length);
        }
    }

    @Override
    public Collection<RowData> lookup(RowData rowData) throws IOException {
        // TODO: Optimize performance when object reuse is enabled.
        GenericRowData result = new GenericRowData(fieldDataTypes.length);

        for (int i = 0; i < logicalKeyFieldIndices.length; i++) {
            result.setField(
                    logicalKeyFieldIndices[i], logicalKeyGetters[i].getFieldOrNull(rowData));
        }

        for (int i = 0; i < featureFieldIndices.length; i++) {
            result.setField(
                    physicalKeyFieldIndices[i], physicalKeyGetters[i].getFieldOrNull(rowData));

            String key =
                    ConversionUtils.toString(
                            rowData,
                            i + logicalKeyFieldIndices.length,
                            fieldDataTypes[physicalKeyFieldIndices[i]]);
            int valueFieldIndex = featureFieldIndices[i];
            DataType valueType = fieldDataTypes[valueFieldIndex];
            if (valueType instanceof CollectionDataType) {
                List<String> redisData = client.lrange(key, 0, -1);
                if (redisData.isEmpty()) {
                    continue;
                }
                ArrayData flinkSqlData =
                        ConversionUtils.fromList(redisData, (CollectionDataType) valueType);
                result.setField(valueFieldIndex, flinkSqlData);
            } else if (valueType instanceof KeyValueDataType) {
                Map<String, String> redisData;
                if (hashFields == null || hashFields[valueFieldIndex] == null) {
                    redisData = client.hgetAll(key);
                } else {
                    redisData = new HashMap<>();
                    List<String> values = client.hmget(key, hashFields[valueFieldIndex]);
                    for (int j = 0; j < hashFields[valueFieldIndex].length; j++) {
                        redisData.put(hashFields[valueFieldIndex][j], values.get(j));
                    }
                }
                if (redisData.isEmpty()) {
                    continue;
                }
                MapData flinkSqlData =
                        ConversionUtils.fromMap(redisData, (KeyValueDataType) valueType);
                result.setField(valueFieldIndex, flinkSqlData);
            } else {
                String redisData = client.get(key);
                if (redisData == null) {
                    continue;
                }
                Object flinkSqlData = ConversionUtils.fromString(redisData, valueType);
                result.setField(valueFieldIndex, flinkSqlData);
            }
        }

        return Collections.singletonList(result);
    }

    @Override
    public void close() throws Exception {
        super.close();
        this.client.close();
    }

    private static int[] getLogicalKeyFieldIndices(ResolvedSchema schema, ReadableConfig config) {
        String[] keyFields = config.get(KEY_FIELDS).split(",");
        List<Integer> indices = new ArrayList<>();
        List<String> fieldNames = schema.getColumnNames();
        for (String keyField : keyFields) {
            int keyFieldIndex = fieldNames.indexOf(keyField);
            Preconditions.checkArgument(
                    keyFieldIndex >= 0, "Input table does not contain key field %s.", keyField);
            indices.add(keyFieldIndex);
        }
        return indices.stream().mapToInt(Integer::intValue).toArray();
    }

    private static int[] getPhysicalKeyFieldIndices(
            ResolvedSchema schema, int[] valueFieldIndices) {
        List<Integer> indices = new ArrayList<>();
        List<String> fieldNames = schema.getColumnNames();
        for (int i : valueFieldIndices) {
            int keyFieldIndex = fieldNames.indexOf("__KEY__" + fieldNames.get(i));
            Preconditions.checkArgument(
                    keyFieldIndex >= 0,
                    "Input table does not contain key field %s.",
                    "__KEY__" + fieldNames.get(i));
            indices.add(keyFieldIndex);
        }
        return indices.stream().mapToInt(Integer::intValue).toArray();
    }

    private static int[] getFeatureFieldIndices(ResolvedSchema schema, ReadableConfig config) {
        List<String> keyFields = Arrays.asList(config.get(KEY_FIELDS).split(","));
        List<Integer> indices = new ArrayList<>();
        List<String> fieldNames = schema.getColumnNames();
        for (int i = 0; i < fieldNames.size(); i++) {
            if (!fieldNames.get(i).startsWith("__KEY__")
                    && !keyFields.contains(fieldNames.get(i))) {
                indices.add(i);
            }
        }
        return indices.stream().mapToInt(Integer::intValue).toArray();
    }
}
