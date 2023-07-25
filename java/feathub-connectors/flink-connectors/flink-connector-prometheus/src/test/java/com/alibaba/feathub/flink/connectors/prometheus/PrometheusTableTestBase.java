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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/** Base class of Prometheus test. It starts a Prometheus PushGateway container. */
public class PrometheusTableTestBase {
    public static final PrometheusPushGatewayContainer PROMETHEUS_PUSH_GATEWAY_CONTAINER =
            new PrometheusPushGatewayContainer();

    @BeforeAll
    static void beforeAll() {
        PROMETHEUS_PUSH_GATEWAY_CONTAINER.start();
    }

    @AfterAll
    static void afterAll() {
        PROMETHEUS_PUSH_GATEWAY_CONTAINER.stop();
    }
}
