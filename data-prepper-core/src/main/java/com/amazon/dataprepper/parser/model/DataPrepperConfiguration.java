/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.parser.model;

import com.amazon.dataprepper.model.configuration.PluginModel;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class to hold configuration for DataPrepper, including server port and Log4j settings
 */
public class DataPrepperConfiguration {
    static final int MAX_TAGS_NUMBER = 3;
    private static final List<MetricRegistryType> DEFAULT_METRIC_REGISTRY_TYPE = Collections.singletonList(MetricRegistryType.Prometheus);
    private int serverPort = 4900;
    private boolean ssl = true;
    private String keyStoreFilePath = "";
    private String keyStorePassword = "";
    private String privateKeyPassword = "";
    private List<MetricRegistryType> metricRegistries = DEFAULT_METRIC_REGISTRY_TYPE;
    private PluginModel authentication;
    private Map<String, String> metricTags = new HashMap<>();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

    public static final DataPrepperConfiguration DEFAULT_CONFIG = new DataPrepperConfiguration();

    public DataPrepperConfiguration() {}

    // TODO: camel case to snake eyes in JsonProperty
    @JsonCreator
    public DataPrepperConfiguration(
            @JsonProperty("ssl") final Boolean ssl,
            @JsonProperty("keyStoreFilePath") final String keyStoreFilePath,
            @JsonProperty("keyStorePassword") final String keyStorePassword,
            @JsonProperty("privateKeyPassword") final String privateKeyPassword,
            @JsonProperty("serverPort") final String serverPort,
            @JsonProperty("metricRegistries") final List<MetricRegistryType> metricRegistries,
            @JsonProperty("authentication") final PluginModel authentication,
            @JsonProperty("metricTags") final Map<String, String> metricTags
            ) {
        this.authentication = authentication;
        setSsl(ssl);
        this.keyStoreFilePath = keyStoreFilePath != null ? keyStoreFilePath : "";
        this.keyStorePassword = keyStorePassword != null ? keyStorePassword : "";
        this.privateKeyPassword = privateKeyPassword != null ? privateKeyPassword : "";
        this.metricRegistries = metricRegistries != null && !metricRegistries.isEmpty() ? metricRegistries : DEFAULT_METRIC_REGISTRY_TYPE;
        setMetricTags(metricTags);
        setServerPort(serverPort);
    }

    public int getServerPort() {
        return serverPort;
    }

    public boolean ssl() {
        return ssl;
    }

    public String getKeyStoreFilePath() {
        return keyStoreFilePath;
    }

    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    public String getPrivateKeyPassword() {
        return privateKeyPassword;
    }

    public List<MetricRegistryType> getMetricRegistryTypes() {
        return metricRegistries;
    }

    public Map<String, String> getMetricTags() {
        return metricTags;
    }

    private void setSsl(final Boolean ssl) {
        if (ssl != null) {
            this.ssl = ssl;
        }
    }

    public PluginModel getAuthentication() {
        return authentication;
    }

    private void setServerPort(final String serverPort) {
        if(serverPort != null && !serverPort.isEmpty()) {
            try {
                int port = Integer.parseInt(serverPort);
                if(port <= 0) {
                    throw new IllegalArgumentException("Server port must be a positive integer");
                }
                this.serverPort = port;
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Server port must be a positive integer");
            }
        }
    }

    private void setMetricTags(final Map<String, String> metricTags) {
        if (metricTags != null) {
            if (metricTags.size() > MAX_TAGS_NUMBER) {
                throw new IllegalArgumentException("metricTags cannot be more than 3");
            }
            this.metricTags = metricTags;
        }
    }
}
