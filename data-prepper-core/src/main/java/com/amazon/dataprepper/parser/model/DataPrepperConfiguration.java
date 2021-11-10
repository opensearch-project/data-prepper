/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.parser.model;

import com.amazon.dataprepper.model.configuration.PluginModel;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Class to hold configuration for DataPrepper, including server port and Log4j settings
 */
public class DataPrepperConfiguration {
    private static final List<MetricRegistryType> DEFAULT_METRIC_REGISTRY_TYPE = Collections.singletonList(MetricRegistryType.Prometheus);
    private int serverPort = 4900;
    private boolean ssl = true;
    private String keyStoreFilePath = "";
    private String keyStorePassword = "";
    private String privateKeyPassword = "";
    private List<MetricRegistryType> metricRegistries = DEFAULT_METRIC_REGISTRY_TYPE;
    private PluginModel authentication;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

    public static final DataPrepperConfiguration DEFAULT_CONFIG = new DataPrepperConfiguration();

    /**
     * Construct a DataPrepperConfiguration from a yaml file
     * @param file
     * @return
     */
    public static DataPrepperConfiguration fromFile(File file) {
        try {
            return OBJECT_MAPPER.readValue(file, DataPrepperConfiguration.class);
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid DataPrepper configuration file.", e);
        }
    }

    private DataPrepperConfiguration() {}

    @JsonCreator
    public DataPrepperConfiguration(
            @JsonProperty("ssl") final Boolean ssl,
            @JsonProperty("keyStoreFilePath") final String keyStoreFilePath,
            @JsonProperty("keyStorePassword") final String keyStorePassword,
            @JsonProperty("privateKeyPassword") final String privateKeyPassword,
            @JsonProperty("serverPort") final String serverPort,
            @JsonProperty("metricRegistries") final List<MetricRegistryType> metricRegistries,
            @JsonProperty("authentication") final PluginModel authentication
            ) {
        this.authentication = authentication;
        setSsl(ssl);
        this.keyStoreFilePath = keyStoreFilePath != null ? keyStoreFilePath : "";
        this.keyStorePassword = keyStorePassword != null ? keyStorePassword : "";
        this.privateKeyPassword = privateKeyPassword != null ? privateKeyPassword : "";
        this.metricRegistries = metricRegistries != null && !metricRegistries.isEmpty() ? metricRegistries : DEFAULT_METRIC_REGISTRY_TYPE;
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
}
