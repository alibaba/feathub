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

import com.github.dockerjava.api.command.InspectContainerResponse;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;

/** Prometheus PushGateway container for testing. */
public class PrometheusPushGatewayContainer
        extends GenericContainer<PrometheusPushGatewayContainer> {

    private static final Logger LOG = LoggerFactory.getLogger(PrometheusPushGatewayContainer.class);

    private static final DockerImageName IMAGE_NAME = DockerImageName.parse("prom/pushgateway");
    private static final String DEFAULT_TAG = "v1.6.0";

    public PrometheusPushGatewayContainer() {
        super(IMAGE_NAME.withTag(DEFAULT_TAG));
        this.withExposedPorts(9091);
    }

    @Override
    protected void containerIsStarted(InspectContainerResponse containerInfo) {
        final String url = getHostUrl();
        LOG.info("PushGateway url: {}", url);
        final PushGateway pushgateway = new PushGateway(url);
        final Gauge gauge = Gauge.build().name("probe").help("probe").register();

        int tryTime = 10;
        Exception exception = null;

        while (tryTime > 0) {
            try {
                pushgateway.push(gauge, "probe-job");
                pushgateway.delete("probe-job");
                LOG.info("PushGateway is ready.");
                return;
            } catch (IOException e) {
                exception = e;
                LOG.info("Waiting for PushGateway to be ready.");
            }
            try {
                Thread.sleep(6000);
            } catch (InterruptedException e) {
                // ignore
            }
            tryTime -= 1;
        }

        throw new RuntimeException("PushGateway is not ready in 1 minute.", exception);
    }

    public String getHostUrl() {
        return String.format("%s:%s", this.getHost(), this.getMappedPort(9091));
    }
}
