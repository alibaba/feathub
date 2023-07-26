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

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The base class for MetricUpdater that update the metric with a list.
 *
 * @param <ELEMENT_T> The element type in the list.
 */
public abstract class AbstractListMetricUpdater<ELEMENT_T>
        implements MetricUpdater<List<ELEMENT_T>> {

    protected final SimpleCollector<Gauge.Child> collector;

    public AbstractListMetricUpdater(
            String name, List<Map<String, String>> labelLists, CollectorRegistry registry) {
        verifyLabelLists(labelLists);

        collector =
                Gauge.build()
                        .name(name)
                        .help(name)
                        .labelNames(labelLists.get(0).keySet().toArray(new String[0]))
                        .register(registry);
    }

    private void verifyLabelLists(List<Map<String, String>> labelLists) {
        if (labelLists.isEmpty()) {
            throw new RuntimeException("List metric must has at least one list of labels.");
        }

        if (labelLists.get(0).isEmpty()) {
            throw new RuntimeException("List metric must has at least one label.");
        }

        final Set<String> keySet = labelLists.get(0).keySet();
        for (Map<String, String> labels : labelLists) {
            if (!keySet.equals(labels.keySet())) {
                throw new RuntimeException("All labels list must have the same set of label keys.");
            }
        }
    }
}
