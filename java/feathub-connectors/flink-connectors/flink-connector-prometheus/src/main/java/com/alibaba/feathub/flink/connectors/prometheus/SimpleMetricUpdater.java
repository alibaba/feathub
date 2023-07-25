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

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.SimpleCollector;

import java.util.Map;

/** The SimpleMetricUpdater updates a Prometheus gauge metric with a Double value. */
public class SimpleMetricUpdater implements MetricUpdater<Double> {

    private final Gauge.Child child;

    public SimpleMetricUpdater(
            String name, Map<String, String> labels, CollectorRegistry registry) {
        SimpleCollector<Gauge.Child> collector =
                Gauge.build()
                        .name(name)
                        .help(name)
                        .labelNames(labels.keySet().toArray(new String[0]))
                        .register(registry);
        child = collector.labels(labels.values().toArray(new String[0]));
    }

    @Override
    public void update(Double value) {
        child.set(value);
    }
}
