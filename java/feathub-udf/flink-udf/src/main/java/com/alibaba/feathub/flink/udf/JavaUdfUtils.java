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

package com.alibaba.feathub.flink.udf;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

/** Utility methods for Java UDFs. */
public class JavaUdfUtils {
    /**
     * Evaluates a Java UDF against a DataStream.
     *
     * @param stream DataStream containing input data for the UDF.
     * @param className The classpath of the UDF to be evaluated. Must be a subclass of {@link
     *     OneInputStreamOperator}.
     * @param parameters The constructor parameters of the UDF.
     * @return DataStream containing output data of the UDF.
     */
    @SuppressWarnings({"unchecked"})
    public static DataStream<Row> evalJavaUdf(
            DataStream<Row> stream, String className, List<?> parameters) throws Exception {
        if (stream.getParallelism() != 1) {
            // The parallelism of currently supported JDFs must be 1.
            throw new UnsupportedOperationException(
                    "The parallelism of the upstream of a Java UDF must be 1.");
        }

        // TODO: Support cases in which the input and output type differs.
        return stream.transform(
                        "JavaUDF",
                        stream.getType(),
                        (OneInputStreamOperator<Row, Row>) instantiate(className, parameters))
                .setParallelism(stream.getParallelism());
    }

    private static Object instantiate(String className, List<?> parameters) throws Exception {
        Class<?> clazz = Class.forName(className);
        List<Class<?>> parameterClasses = new ArrayList<>();
        for (Object parameter : parameters) {
            parameterClasses.add(parameter.getClass());
        }
        return clazz.getConstructor(parameterClasses.toArray(new Class[0]))
                .newInstance(parameters.toArray());
    }
}
