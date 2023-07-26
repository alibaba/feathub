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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The MapListMetricUpdater updates Prometheus gauge metrics with a list of maps that map from
 * string to double.
 */
public class MapListMetricUpdater extends AbstractListMetricUpdater<Map<String, Double>> {

    private final List<Map<String, Gauge.Child>> childrenList;
    private final List<Map<String, String>> labelLists;

    public MapListMetricUpdater(
            String name, List<Map<String, String>> labelLists, CollectorRegistry registry) {
        super(name, labelLists, registry);

        this.labelLists = labelLists;
        this.childrenList = new ArrayList<>();
        for (int i = 0; i < labelLists.size(); ++i) {
            childrenList.add(new HashMap<>());
        }
    }

    Gauge.Child getChild(Integer index, String mapKey) {
        final Map<String, Gauge.Child> children = childrenList.get(index);
        return children.computeIfAbsent(
                mapKey,
                label -> {
                    final List<String> labelValues = new ArrayList<>();
                    for (String labelValue : labelLists.get(index).values()) {
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
    public void update(List<Map<String, Double>> values) {
        if (values.size() != childrenList.size()) {
            throw new RuntimeException("List metric size must be the same as the labels set size.");
        }

        for (int i = 0; i < values.size(); ++i) {
            for (Map.Entry<String, Double> entry : values.get(i).entrySet()) {
                final String labelValue = entry.getKey();
                final Double metricValue = entry.getValue();
                getChild(i, labelValue).set(metricValue);
            }
        }
    }
}
