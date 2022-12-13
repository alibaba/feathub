/*
 * Copyright 2022 The Feathub Authors
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
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import redis.clients.jedis.Jedis;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.feathub.flink.connectors.redis.sink.RedisSinkConfigs.DB_NUM;
import static com.alibaba.feathub.flink.connectors.redis.sink.RedisSinkConfigs.HOST;
import static com.alibaba.feathub.flink.connectors.redis.sink.RedisSinkConfigs.KEY_FIELD;
import static com.alibaba.feathub.flink.connectors.redis.sink.RedisSinkConfigs.NAMESPACE;
import static com.alibaba.feathub.flink.connectors.redis.sink.RedisSinkConfigs.PASSWORD;
import static com.alibaba.feathub.flink.connectors.redis.sink.RedisSinkConfigs.PORT;
import static com.alibaba.feathub.flink.connectors.redis.sink.RedisSinkConfigs.TIMESTAMP_FIELD;
import static com.alibaba.feathub.flink.connectors.redis.sink.RedisSinkConfigs.USERNAME;

/**
 * A {@link org.apache.flink.streaming.api.functions.sink.SinkFunction} that writes to a Redis
 * database.
 *
 * <p>The timestamp field, if specified, must contain Long values representing milliseconds from
 * epoch, and the other fields must contain byte arrays representing serialized key or data.
 */
public class RedisSinkFunction extends RichSinkFunction<RowData> {

    private final String host;
    private final int port;
    private final String username;
    private final String password;
    private final int dbNum;

    private final byte[] keyPrefix;
    private final int keyFieldIndex;
    private final int timestampFieldIndex;

    private final byte[] evalScript;
    private byte[] evalScriptSHA;

    private transient ByteBuffer indexBuffer;
    private transient ByteBuffer timestampBuffer;

    private transient Jedis jedis;

    public RedisSinkFunction(ReadableConfig config, ResolvedSchema schema) {
        this.host = config.get(HOST);
        this.port = config.get(PORT);
        this.username = config.get(USERNAME);
        this.password = config.get(PASSWORD);
        this.dbNum = config.get(DB_NUM);

        this.keyPrefix = getKeyPrefix(config.get(NAMESPACE));
        this.keyFieldIndex = getKeyFieldIndex(config, schema);
        this.timestampFieldIndex = getTimestampFieldIndex(config, schema);

        this.evalScript = getEvalScript(timestampFieldIndex);

        validateConfigAndSchema(config, schema, keyFieldIndex, timestampFieldIndex);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        this.indexBuffer = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
        this.timestampBuffer = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);

        jedis = new Jedis(host, port);
        if (!StringUtils.isNullOrWhitespaceOnly(username)) {
            jedis.auth(username, password);
        } else if (!StringUtils.isNullOrWhitespaceOnly(password)) {
            jedis.auth(password);
        }
        jedis.select(dbNum);
        if (timestampFieldIndex >= 0) {
            evalScriptSHA = jedis.scriptLoad(this.evalScript);
        }
    }

    @Override
    public void invoke(RowData data, Context context) {
        byte[] originalKey = data.getBinary(keyFieldIndex);
        byte[] key = new byte[keyPrefix.length + originalKey.length];
        System.arraycopy(keyPrefix, 0, key, 0, keyPrefix.length);
        System.arraycopy(originalKey, 0, key, keyPrefix.length, originalKey.length);

        Map<byte[], byte[]> value = new HashMap<>();
        int arity = data.getArity();
        for (int i = 0; i < arity; i++) {
            if (timestampFieldIndex == i) {
                // timestamp values have specific serialization strategy
                continue;
            }

            if (keyFieldIndex == i) {
                // don't store key values
                continue;
            }

            indexBuffer.putInt(0, i);
            value.put(indexBuffer.array(), data.getBinary(i));
        }

        if (timestampFieldIndex < 0) {
            this.jedis.hset(key, value);
        } else {
            timestampBuffer.putLong(0, data.getLong(timestampFieldIndex));

            List<byte[]> arguments = new ArrayList<>();
            arguments.add(key);
            arguments.add(timestampBuffer.array());
            for (Map.Entry<byte[], byte[]> entry : value.entrySet()) {
                arguments.add(entry.getKey());
                arguments.add(entry.getValue());
            }

            this.jedis.evalsha(this.evalScriptSHA, Collections.emptyList(), arguments);
        }
    }

    @Override
    public void close() {
        // TODO: Remove the registered script from Redis when Redis supports removing a certain
        //  script.
        jedis.close();
    }

    private static byte[] getKeyPrefix(String namespace) {
        Preconditions.checkArgument(
                namespace.matches("^[A-Za-z0-9][A-Za-z0-9_]*$"),
                "Namespace %s should only contain letters, numbers and underscore, "
                        + "and should not start with underscore.");

        return (namespace + ":").getBytes(StandardCharsets.UTF_8);
    }

    private static int getKeyFieldIndex(ReadableConfig config, ResolvedSchema schema) {
        String keyFieldName = config.get(KEY_FIELD);

        List<String> fieldNames = schema.getColumnNames();
        int index = fieldNames.indexOf(keyFieldName);
        Preconditions.checkArgument(
                index >= 0, "Input table does not contain key field %s.", keyFieldName);
        return index;
    }

    private static int getTimestampFieldIndex(ReadableConfig config, ResolvedSchema schema) {
        String timestampFieldName = config.get(TIMESTAMP_FIELD);
        if (StringUtils.isNullOrWhitespaceOnly(timestampFieldName)) {
            return -1;
        }

        List<String> fieldNames = schema.getColumnNames();
        int index = fieldNames.indexOf(timestampFieldName);
        Preconditions.checkArgument(
                index >= 0, "Input table does not contain timestamp field %s.", timestampFieldName);
        return index;
    }

    private static byte[] getEvalScript(int timestampFieldIndex) {
        if (timestampFieldIndex < 0) {
            return new byte[0];
        }

        InputStream in =
                RedisSinkFunction.class
                        .getClassLoader()
                        .getResourceAsStream(
                                "com/alibaba/feathub/flink/connectors/redis/sink/script.lua");

        String script =
                new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))
                        .lines()
                        .reduce((x, y) -> x + "\n" + y)
                        .get();

        return script.getBytes(StandardCharsets.UTF_8);
    }

    private static void validateConfigAndSchema(
            ReadableConfig config,
            ResolvedSchema schema,
            int keyFieldIndex,
            int timestampFieldIndex) {
        Preconditions.checkArgument(
                keyFieldIndex != timestampFieldIndex,
                "Timestamp field %s cannot be a key field.",
                config.get(TIMESTAMP_FIELD));

        int valueFieldNum = schema.getColumnNames().size() - 1;
        if (timestampFieldIndex >= 0) {
            valueFieldNum -= 1;
        }
        Preconditions.checkArgument(
                valueFieldNum > 0,
                "It is not allowed to treat all columns in the input table as key field or timestamp field.");
    }
}
