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

package com.alibaba.feathub.flink.connectors.redis.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A {@link org.apache.flink.streaming.api.functions.sink.SinkFunction} that writes to a Redis
 * database.
 *
 * <p>The input table should contain an even number of columns. half of the columns should have the
 * prefix "__KEY__" in their column names and, once the prefix is removed, their columns names
 * should be equal to those of the other half. Except the fields starting with "__KEY__", each field
 * in the input row would be saved as an individual entry into Redis. The value of the entry is the
 * value of this field, and the key of the entry is the value of the corresponding field starting
 * with "__KEY__".
 */
public class RedisSinkFunction extends RichSinkFunction<RowData> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final ReadableConfig config;
    private final int[] keyFieldIndices;
    private final int[] valueFieldIndices;

    private final DataType[] fieldTypes;

    private transient JedisClient client;

    public RedisSinkFunction(ReadableConfig config, ResolvedSchema schema) {
        this.config = config;

        this.fieldTypes = schema.getColumnDataTypes().toArray(new DataType[0]);
        this.valueFieldIndices = getValueFieldIndices(schema);
        this.keyFieldIndices = getKeyFieldIndices(schema, valueFieldIndices);

        Preconditions.checkArgument(
                schema.getColumnCount() % 2 == 0,
                "Input table should contain an even number of columns.");

        Preconditions.checkArgument(
                valueFieldIndices.length == schema.getColumnCount() / 2,
                "Half of the columns in the input table should not have their names starting with \"__KEY__\".");
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        client = JedisClient.create(this.config);
    }

    @Override
    public void invoke(RowData data, Context context) throws JsonProcessingException {
        for (int i = 0; i < valueFieldIndices.length; i++) {
            String key = data.getString(keyFieldIndices[i]).toString();
            int valueFieldIndex = valueFieldIndices[i];
            DataType dataType = fieldTypes[valueFieldIndex];
            if (dataType instanceof KeyValueDataType) {
                Set<String> oldKeys = client.hkeys(key);
                Map<String, String> newMap =
                        getMap(data.getMap(valueFieldIndex), (KeyValueDataType) dataType);
                Set<String> keysToRemove = new HashSet<>(oldKeys);
                keysToRemove.removeAll(newMap.keySet());
                if (keysToRemove.equals(oldKeys)) {
                    client.del(key);
                } else {
                    client.hdel(key, keysToRemove.toArray(new String[0]));
                }
                client.hmset(key, newMap);
            } else if (dataType instanceof CollectionDataType) {
                List<String> oldList = client.lrange(key, 0, -1);
                List<String> newList =
                        getList(data.getArray(valueFieldIndex), (CollectionDataType) dataType);
                int[] subListInfo = getLongestCommonSubList(oldList, newList);
                int oldStartIndex = subListInfo[0];
                int newStartIndex = subListInfo[1];
                int subListLength = subListInfo[2];
                if (subListLength == 0) {
                    client.del(key);
                    client.rpush(key, newList.toArray(new String[0]));
                } else {
                    if (oldStartIndex > 0) {
                        client.lpop(key, oldStartIndex);
                    }

                    if (oldList.size() - oldStartIndex - subListLength > 0) {
                        client.rpop(key, oldList.size() - oldStartIndex - subListLength);
                    }

                    if (newStartIndex > 0) {
                        client.lpush(key, newList.subList(0, newStartIndex).toArray(new String[0]));
                    }

                    if (newStartIndex + subListLength < newList.size()) {
                        client.rpush(
                                key,
                                newList.subList(newStartIndex + subListLength, newList.size())
                                        .toArray(new String[0]));
                    }
                }
            } else {
                client.set(key, getString(data, valueFieldIndex, dataType));
            }
        }

        client.flush();
    }

    @Override
    public void close() {
        // TODO: Remove the registered script from Redis when Redis supports removing a certain
        //  script.
        client.close();
    }

    /**
     * Get the longest common sublist between two input lists. Used to minimize operations to update
     * a Redis LIST.
     *
     * @return an int array containing the start index in list A, the start index in list B, and the
     *     length of the sublist.
     */
    private static int[] getLongestCommonSubList(List<String> listA, List<String> listB) {
        int maxSubListLen = 0;
        int startIndexA = -1;
        int startIndexB = -1;

        int sizeA = listA.size();
        int sizeB = listB.size();
        int[][] dp = new int[sizeA + 1][sizeB + 1];

        for (int i = 0; i <= sizeA; i++) {
            for (int j = 0; j <= sizeB; j++) {
                if (i == 0 || j == 0) {
                    dp[i][j] = 0;
                } else if (Objects.equals(listA.get(i - 1), listB.get(j - 1))) {
                    dp[i][j] = 1 + dp[i - 1][j - 1];
                } else {
                    dp[i][j] = 0;
                }

                if (maxSubListLen < dp[i][j]) {
                    maxSubListLen = dp[i][j];
                    startIndexA = i - maxSubListLen;
                    startIndexB = j - maxSubListLen;
                }
            }
        }

        return new int[] {startIndexA, startIndexB, maxSubListLen};
    }

    private static String getString(RowData data, int index, DataType dataType)
            throws JsonProcessingException {
        if (dataType instanceof AtomicDataType) {
            LogicalType logicalType = dataType.getLogicalType();
            if (logicalType instanceof VarCharType) {
                return data.getString(index).toString();
            } else if (logicalType instanceof VarBinaryType) {
                return new String(data.getBinary(index));
            } else if (logicalType instanceof IntType) {
                return Integer.toString(data.getInt(index));
            } else if (logicalType instanceof BigIntType) {
                return Long.toString(data.getLong(index));
            } else if (logicalType instanceof DoubleType) {
                return Double.toString(data.getDouble(index));
            } else if (logicalType instanceof FloatType) {
                return Float.toString(data.getFloat(index));
            } else if (logicalType instanceof BooleanType) {
                return Boolean.toString(data.getBoolean(index));
            } else if (logicalType instanceof TimestampType) {
                return Long.toString(
                        data.getTimestamp(index, ((TimestampType) logicalType).getPrecision())
                                .getMillisecond());
            }

            throw new UnsupportedOperationException(
                    String.format(
                            "Cannot write data with type %s to Redis.",
                            dataType.getLogicalType().getClass().getName()));

        } else if (dataType instanceof KeyValueDataType) {
            return OBJECT_MAPPER.writeValueAsString(
                    getMap(data.getMap(index), (KeyValueDataType) dataType));
        } else if (dataType instanceof CollectionDataType) {
            return OBJECT_MAPPER.writeValueAsString(
                    getList(data.getArray(index), (CollectionDataType) dataType));
        }

        throw new UnsupportedOperationException(
                String.format(
                        "Cannot write data with type %s to Redis.", dataType.getClass().getName()));
    }

    private static String getString(ArrayData data, int index, DataType dataType)
            throws JsonProcessingException {
        if (dataType instanceof AtomicDataType) {
            LogicalType logicalType = dataType.getLogicalType();
            if (logicalType instanceof VarCharType) {
                return data.getString(index).toString();
            } else if (logicalType instanceof VarBinaryType) {
                return new String(data.getBinary(index));
            } else if (logicalType instanceof IntType) {
                return Integer.toString(data.getInt(index));
            } else if (logicalType instanceof BigIntType) {
                return Long.toString(data.getLong(index));
            } else if (logicalType instanceof DoubleType) {
                return Double.toString(data.getDouble(index));
            } else if (logicalType instanceof FloatType) {
                return Float.toString(data.getFloat(index));
            } else if (logicalType instanceof BooleanType) {
                return Boolean.toString(data.getBoolean(index));
            } else if (logicalType instanceof TimestampType) {
                return Long.toString(
                        data.getTimestamp(index, ((TimestampType) logicalType).getPrecision())
                                .getMillisecond());
            }

            throw new UnsupportedOperationException(
                    String.format(
                            "Cannot write data with type %s to Redis.",
                            dataType.getLogicalType().getClass().getName()));

        } else if (dataType instanceof KeyValueDataType) {
            return OBJECT_MAPPER.writeValueAsString(
                    getMap(data.getMap(index), (KeyValueDataType) dataType));
        } else if (dataType instanceof CollectionDataType) {
            return OBJECT_MAPPER.writeValueAsString(
                    getList(data.getArray(index), (CollectionDataType) dataType));
        }

        throw new UnsupportedOperationException(
                String.format(
                        "Cannot write data with type %s to Redis.", dataType.getClass().getName()));
    }

    private static Map<String, String> getMap(MapData mapData, KeyValueDataType dataType)
            throws JsonProcessingException {
        ArrayData keyArrayData = mapData.keyArray();
        DataType keyDataType = dataType.getKeyDataType();
        ArrayData valueArrayData = mapData.valueArray();
        DataType valueDataType = dataType.getValueDataType();
        int size = keyArrayData.size();
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < size; i++) {
            map.put(
                    getString(keyArrayData, i, keyDataType),
                    getString(valueArrayData, i, valueDataType));
        }
        return map;
    }

    private static List<String> getList(ArrayData arrayData, CollectionDataType dataType)
            throws JsonProcessingException {
        List<String> list = new ArrayList<>();
        DataType elementDataType = dataType.getElementDataType();
        int size = arrayData.size();
        for (int i = 0; i < size; i++) {
            list.add(getString(arrayData, i, elementDataType));
        }
        return list;
    }

    private static int[] getKeyFieldIndices(ResolvedSchema schema, int[] valueFieldIndices) {
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

    private static int[] getValueFieldIndices(ResolvedSchema schema) {
        List<Integer> indices = new ArrayList<>();
        List<String> fieldNames = schema.getColumnNames();
        for (int i = 0; i < schema.getColumnCount(); i++) {
            if (!fieldNames.get(i).startsWith("__KEY__")) {
                indices.add(i);
            }
        }
        return indices.stream().mapToInt(Integer::intValue).toArray();
    }
}
