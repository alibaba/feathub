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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/** The MapMetricUpdater updates Prometheus gauge metrics with a Map from String to Double. */
public class MapMetricUpdater implements MetricUpdater<Map<String, Double>> {

    private final Map<String, String> labels;
    private final Map<String, Gauge.Child> children;
    private final SimpleCollector<Gauge.Child> collector;

    public MapMetricUpdater(String name, Map<String, String> labels, CollectorRegistry registry) {
        verifyLabels(labels);
        this.labels = labels;
        this.children = new HashMap<>();
        this.collector =
                Gauge.build()
                        .name(name)
                        .help(name)
                        .labelNames(labels.keySet().toArray(new String[0]))
                        .register(registry);
    }

    Gauge.Child getChild(String mapKey) {
        return children.computeIfAbsent(
                mapKey,
                label -> {
                    final ArrayList<String> labelValues = new ArrayList<>(this.labels.size());
                    for (String labelValue : labels.values()) {
                        if (labelValue.equals("null")) {
                            labelValues.add(mapKey);
                        } else {
                            labelValues.add(labelValue);
                        }
                    }
                    return collector.labels(labelValues.toArray(new String[0]));
                });
    }

    @Override
    public void update(Map<String, Double> value) {
        for (Map.Entry<String, Double> entry : value.entrySet()) {
            final String labelValue = entry.getKey();
            final Double metricValue = entry.getValue();
            getChild(labelValue).set(metricValue);
        }
    }

    private void verifyLabels(Map<String, String> labels) {
        if (labels.values().stream().noneMatch(v -> v.equals("null"))) {
            throw new RuntimeException("There must be at least one label with null value.");
        }
    }
}
