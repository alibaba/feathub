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
import java.util.List;
import java.util.Map;

/** The DoubleListMetricUpdater updates Prometheus gauge metrics with a list of double values. */
public class DoubleListMetricUpdater extends AbstractListMetricUpdater<Double> {

    private final List<Gauge.Child> children;

    public DoubleListMetricUpdater(
            String name, List<Map<String, String>> labelLists, CollectorRegistry registry) {
        super(name, labelLists, registry);

        this.children = new ArrayList<>();
        for (Map<String, String> labelsSet : labelLists) {
            children.add(collector.labels(labelsSet.values().toArray(new String[0])));
        }
    }

    @Override
    public void update(List<Double> values) {
        if (values.size() != children.size()) {
            throw new RuntimeException(
                    "List metric size must be the same as the number of label lists.");
        }
        for (int i = 0; i < values.size(); ++i) {
            children.get(i).set(values.get(i));
        }
    }
}
