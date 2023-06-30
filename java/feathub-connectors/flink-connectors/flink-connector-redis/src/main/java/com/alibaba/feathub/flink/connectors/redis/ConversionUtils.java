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

package com.alibaba.feathub.flink.connectors.redis;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class containing utility methods to convert data from arbitrary flink RowData element types to
 * Redis-supported data types and backwards.
 */
public class ConversionUtils {
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static String toString(RowData data, int index, DataType dataType)
            throws JsonProcessingException {
        if (data.isNullAt(index)) {
            return null;
        }

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
                    toMap(data.getMap(index), (KeyValueDataType) dataType));
        } else if (dataType instanceof CollectionDataType) {
            return OBJECT_MAPPER.writeValueAsString(
                    toList(data.getArray(index), (CollectionDataType) dataType));
        }

        throw new UnsupportedOperationException(
                String.format(
                        "Cannot write data with type %s to Redis.", dataType.getClass().getName()));
    }

    public static String toString(ArrayData data, int index, DataType dataType)
            throws JsonProcessingException {
        if (data.isNullAt(index)) {
            return null;
        }

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
                    toMap(data.getMap(index), (KeyValueDataType) dataType));
        } else if (dataType instanceof CollectionDataType) {
            return OBJECT_MAPPER.writeValueAsString(
                    toList(data.getArray(index), (CollectionDataType) dataType));
        }

        throw new UnsupportedOperationException(
                String.format(
                        "Cannot write data with type %s to Redis.", dataType.getClass().getName()));
    }

    public static Map<String, String> toMap(MapData mapData, KeyValueDataType dataType)
            throws JsonProcessingException {
        if (mapData == null) {
            return null;
        }

        ArrayData keyArrayData = mapData.keyArray();
        DataType keyDataType = dataType.getKeyDataType();
        ArrayData valueArrayData = mapData.valueArray();
        DataType valueDataType = dataType.getValueDataType();
        int size = keyArrayData.size();
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < size; i++) {
            map.put(
                    toString(keyArrayData, i, keyDataType),
                    toString(valueArrayData, i, valueDataType));
        }
        return map;
    }

    public static List<String> toList(ArrayData arrayData, CollectionDataType dataType)
            throws JsonProcessingException {
        if (arrayData == null) {
            return null;
        }

        List<String> list = new ArrayList<>();
        DataType elementDataType = dataType.getElementDataType();
        int size = arrayData.size();
        for (int i = 0; i < size; i++) {
            list.add(toString(arrayData, i, elementDataType));
        }
        return list;
    }

    public static Object fromString(String string, DataType dataType)
            throws JsonProcessingException {
        if (string == null) {
            return null;
        }

        if (dataType instanceof AtomicDataType) {
            LogicalType logicalType = dataType.getLogicalType();
            if (logicalType instanceof VarCharType) {
                return StringData.fromString(string);
            } else if (logicalType instanceof VarBinaryType) {
                return string.getBytes();
            } else if (logicalType instanceof IntType) {
                return Integer.parseInt(string);
            } else if (logicalType instanceof BigIntType) {
                return Long.parseLong(string);
            } else if (logicalType instanceof DoubleType) {
                return Double.parseDouble(string);
            } else if (logicalType instanceof FloatType) {
                return Float.parseFloat(string);
            } else if (logicalType instanceof BooleanType) {
                return Boolean.parseBoolean(string);
            } else if (logicalType instanceof TimestampType) {
                return TimestampData.fromEpochMillis(Long.parseLong(string));
            }

            throw new UnsupportedOperationException(
                    String.format(
                            "Cannot write data with type %s to Redis.",
                            dataType.getLogicalType().getClass().getName()));

        } else if (dataType instanceof KeyValueDataType) {
            return fromMap(
                    (Map<String, String>) OBJECT_MAPPER.readValue(string, Map.class),
                    (KeyValueDataType) dataType);
        } else if (dataType instanceof CollectionDataType) {
            return fromList(
                    (List<String>) OBJECT_MAPPER.readValue(string, List.class),
                    (CollectionDataType) dataType);
        }

        throw new UnsupportedOperationException(
                String.format(
                        "Cannot write data with type %s to Redis.", dataType.getClass().getName()));
    }

    public static ArrayData fromList(List<String> list, CollectionDataType dataType)
            throws JsonProcessingException {
        if (list == null) {
            return null;
        }

        DataType elementDataType = dataType.getElementDataType();
        Object[] objects = new Object[list.size()];
        for (int i = 0; i < list.size(); i++) {
            objects[i] = fromString(list.get(i), elementDataType);
        }
        return new GenericArrayData(objects);
    }

    public static MapData fromMap(Map<String, String> map, KeyValueDataType dataType)
            throws JsonProcessingException {
        if (map == null) {
            return null;
        }

        DataType keyDataType = dataType.getKeyDataType();
        DataType valueDataType = dataType.getValueDataType();
        Map<Object, Object> newMap = new HashMap<>();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            newMap.put(
                    fromString(entry.getKey(), keyDataType),
                    fromString(entry.getValue(), valueDataType));
        }
        return new GenericMapData(newMap);
    }
}
