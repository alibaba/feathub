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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

import static com.alibaba.feathub.flink.connectors.prometheus.PrometheusConfigs.DELETE_ON_SHUTDOWN;
import static com.alibaba.feathub.flink.connectors.prometheus.PrometheusConfigs.EXTRA_LABELS;
import static com.alibaba.feathub.flink.connectors.prometheus.PrometheusConfigs.JOB_NAME;
import static com.alibaba.feathub.flink.connectors.prometheus.PrometheusConfigs.SERVER_URL;

/** The table factory for {@link PrometheusDynamicTableSink}. */
public class PrometheusDynamicTableFactory implements DynamicTableSinkFactory {
    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        return new PrometheusDynamicTableSink(
                helper.getOptions(), context.getCatalogTable().getResolvedSchema());
    }

    @Override
    public String factoryIdentifier() {
        return "prometheus";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SERVER_URL);
        options.add(JOB_NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(EXTRA_LABELS);
        options.add(DELETE_ON_SHUTDOWN);
        return options;
    }
}
