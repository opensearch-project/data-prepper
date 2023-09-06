/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafkaconnect.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.opensearch.dataprepper.plugins.kafkaconnect.util.Connector;

import java.util.List;
import java.util.Properties;

public abstract class ConnectorConfig {
    @JsonProperty("force_update")
    public Boolean forceUpdate = false;
    private String bootstrapServers;
    private Properties authProperties;

    public abstract List<Connector> buildConnectors();

    public Properties getAuthProperties() {
        return this.authProperties;
    }

    public void setAuthProperties(Properties authProperties) {
        this.authProperties = authProperties;
    }

    public String getBootstrapServers() {
        return this.bootstrapServers;
    }

    public void setBootstrapServers(String bootStrapServers) {
        this.bootstrapServers = bootStrapServers;
    }
}
