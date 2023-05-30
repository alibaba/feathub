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

package org.apache.flink.streaming.connectors.redis;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.KeyValueDataType;

import com.alibaba.feathub.flink.connectors.redis.ConversionUtils;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests {@link ConversionUtils}. */
public class ConversionUtilsTest {
    @Test
    public void test() throws Exception {
        List<Tuple3<DataType, Object, Object>> testData =
                Arrays.asList(
                        Tuple3.of(DataTypes.STRING(), StringData.fromString("foobar"), "foobar"),
                        Tuple3.of(DataTypes.BYTES(), "foobar".getBytes(), "foobar"),
                        Tuple3.of(DataTypes.INT(), 1, "1"),
                        Tuple3.of(DataTypes.BIGINT(), 1L, "1"),
                        Tuple3.of(DataTypes.DOUBLE(), 1.0, "1.0"),
                        Tuple3.of(DataTypes.FLOAT(), 1.0f, "1.0"),
                        Tuple3.of(
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                                new GenericMapData(
                                        new HashMap<StringData, Integer>() {
                                            {
                                                put(StringData.fromString("a"), 1);
                                                put(StringData.fromString("b"), 2);
                                            }
                                        }),
                                new HashMap<String, String>() {
                                    {
                                        put("a", "1");
                                        put("b", "2");
                                    }
                                }),
                        Tuple3.of(
                                DataTypes.MAP(
                                        DataTypes.STRING(),
                                        DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())),
                                new GenericMapData(
                                        new HashMap<StringData, GenericMapData>() {
                                            {
                                                put(
                                                        StringData.fromString("a"),
                                                        new GenericMapData(
                                                                new HashMap<StringData, Integer>() {
                                                                    {
                                                                        put(
                                                                                StringData
                                                                                        .fromString(
                                                                                                "c"),
                                                                                1);
                                                                        put(
                                                                                StringData
                                                                                        .fromString(
                                                                                                "d"),
                                                                                2);
                                                                    }
                                                                }));
                                                put(
                                                        StringData.fromString("b"),
                                                        new GenericMapData(
                                                                new HashMap<StringData, Integer>() {
                                                                    {
                                                                        put(
                                                                                StringData
                                                                                        .fromString(
                                                                                                "e"),
                                                                                3);
                                                                        put(
                                                                                StringData
                                                                                        .fromString(
                                                                                                "f"),
                                                                                4);
                                                                    }
                                                                }));
                                            }
                                        }),
                                new HashMap<String, String>() {
                                    {
                                        put("a", "{\"c\":\"1\",\"d\":\"2\"}");
                                        put("b", "{\"e\":\"3\",\"f\":\"4\"}");
                                    }
                                }),
                        Tuple3.of(
                                DataTypes.ARRAY(DataTypes.STRING()),
                                new GenericArrayData(
                                        new StringData[] {
                                            StringData.fromString("a"), StringData.fromString("b")
                                        }),
                                Arrays.asList("a", "b")),
                        Tuple3.of(
                                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.STRING())),
                                new GenericArrayData(
                                        new GenericArrayData[] {
                                            new GenericArrayData(
                                                    new StringData[] {
                                                        StringData.fromString("a"),
                                                        StringData.fromString("b")
                                                    }),
                                            new GenericArrayData(
                                                    new StringData[] {
                                                        StringData.fromString("c"),
                                                        StringData.fromString("d")
                                                    })
                                        }),
                                Arrays.asList("[\"a\",\"b\"]", "[\"c\",\"d\"]")));

        for (Tuple3<DataType, Object, Object> tuple3 : testData) {
            DataType dataType = tuple3.f0;
            Object sqlData = tuple3.f1;
            Object redisData = tuple3.f2;

            if (redisData instanceof Map) {
                assertThat(redisData)
                        .isEqualTo(
                                ConversionUtils.toMap(
                                        (MapData) sqlData, (KeyValueDataType) dataType));
                assertThat(sqlData)
                        .isEqualTo(
                                ConversionUtils.fromMap(
                                        (Map<String, String>) redisData,
                                        (KeyValueDataType) dataType));

            } else if (redisData instanceof List) {
                assertThat(redisData)
                        .isEqualTo(
                                ConversionUtils.toList(
                                        (ArrayData) sqlData, (CollectionDataType) dataType));
                assertThat(sqlData)
                        .isEqualTo(
                                ConversionUtils.fromList(
                                        (List<String>) redisData, (CollectionDataType) dataType));
            } else {
                GenericRowData rowData = new GenericRowData(1);
                rowData.setField(0, sqlData);
                assertThat(redisData).isEqualTo(ConversionUtils.toString(rowData, 0, dataType));
                assertThat(sqlData)
                        .isEqualTo(ConversionUtils.fromString((String) redisData, dataType));
            }
        }

        Set<DataType> dataTypes = new HashSet<>();
        testData.forEach(
                x -> {
                    if (!((x.f0 instanceof KeyValueDataType)
                            || (x.f0 instanceof CollectionDataType))) {
                        dataTypes.add(x.f0);
                    }
                });
        for (DataType dataType : dataTypes) {
            GenericRowData rowData = new GenericRowData(1);
            assertThat(ConversionUtils.toString(rowData, 0, dataType)).isNull();
            assertThat(ConversionUtils.fromString(null, dataType)).isNull();
        }
    }
}
