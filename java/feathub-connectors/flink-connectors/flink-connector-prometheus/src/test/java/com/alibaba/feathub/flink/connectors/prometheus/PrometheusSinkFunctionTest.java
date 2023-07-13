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

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.alibaba.feathub.flink.connectors.prometheus.PrometheusSinkFunction.parseLabelLists;
import static com.alibaba.feathub.flink.connectors.prometheus.PrometheusSinkFunction.parseLabels;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

class PrometheusSinkFunctionTest {

    @Test
    void testParseLabelLists() {
        Map<String, String> map1 = new LinkedHashMap<>();
        map1.put("k1", "v1");
        map1.put("k2", ",;=");
        Map<String, String> map2 = new LinkedHashMap<>();
        map2.put("k1", ",;=");
        map2.put("k2", "v2");
        assertThat(parseLabelLists("k1=v1,k2=\\,\\;\\=;k1=\\,\\;\\=,k2=v2")).contains(map1, map2);
    }

    @Test
    void testParseLabels() {
        assertThat(parseLabels("k1=v1,k2=\\,\\=")).contains(entry("k1", "v1"), entry("k2", ",="));
    }
}
