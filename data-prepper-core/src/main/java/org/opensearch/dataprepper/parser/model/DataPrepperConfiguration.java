/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.parser.model;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.parser.config.MetricTagFilter;
import org.opensearch.dataprepper.peerforwarder.PeerForwarderConfiguration;
import org.opensearch.dataprepper.pipeline.PipelineShutdownOption;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Class to hold configuration for DataPrepper, including server port and Log4j settings
 */
public class DataPrepperConfiguration {
    static final Duration DEFAULT_SHUTDOWN_DURATION = Duration.ofSeconds(30L);

    static final int MAX_TAGS_NUMBER = 3;
    private static final List<MetricRegistryType> DEFAULT_METRIC_REGISTRY_TYPE = Collections.singletonList(MetricRegistryType.Prometheus);
    private static final PipelineShutdownOption DEFAULT_PIPELINE_SHUTDOWN = PipelineShutdownOption.ON_ANY_PIPELINE_FAILURE;
    private int serverPort = 4900;
    private boolean ssl = true;
    private String keyStoreFilePath = "";
    private String keyStorePassword = "";
    private String privateKeyPassword = "";
    private List<MetricRegistryType> metricRegistries = DEFAULT_METRIC_REGISTRY_TYPE;
    private PluginModel authentication;
    private CircuitBreakerConfig circuitBreakerConfig;
    private SourceCoordinationConfig sourceCoordinationConfig;
    private PipelineShutdownOption pipelineShutdown;
    private Map<String, String> metricTags = new HashMap<>();
    private List<MetricTagFilter> metricTagFilters = new LinkedList<>();
    private PeerForwarderConfiguration peerForwarderConfiguration;
    private Duration processorShutdownTimeout;
    private Duration sinkShutdownTimeout;

    public static final DataPrepperConfiguration DEFAULT_CONFIG = new DataPrepperConfiguration();

    public DataPrepperConfiguration() {}

    @JsonCreator
    public DataPrepperConfiguration(
            @JsonProperty("ssl") final Boolean ssl,
            @JsonProperty("key_store_file_path")
            @JsonAlias("keyStoreFilePath")
            final String keyStoreFilePath,
            @JsonProperty("key_store_password")
            @JsonAlias("keyStorePassword")
            final String keyStorePassword,
            @JsonProperty("private_key_password")
            @JsonAlias("privateKeyPassword")
            final String privateKeyPassword,
            @JsonProperty("server_port")
            @JsonAlias("serverPort")
            final String serverPort,
            @JsonProperty("metric_registries")
            @JsonAlias("metricRegistries")
            final List<MetricRegistryType> metricRegistries,
            @JsonProperty("authentication") final PluginModel authentication,
            @JsonProperty("metric_tags")
            @JsonAlias("metricTags")
            final Map<String, String> metricTags,
            @JsonProperty("metric_tag_filters")
            final List<MetricTagFilter> metricTagFilters,
            @JsonProperty("peer_forwarder") final PeerForwarderConfiguration peerForwarderConfiguration,
            @JsonProperty("processor_shutdown_timeout")
            @JsonAlias("processorShutdownTimeout")
            final Duration processorShutdownTimeout,
            @JsonProperty("sink_shutdown_timeout")
            @JsonAlias("sinkShutdownTimeout")
            final Duration sinkShutdownTimeout,
            @JsonProperty("circuit_breakers") final CircuitBreakerConfig circuitBreakerConfig,
            @JsonProperty("source_coordination") final SourceCoordinationConfig sourceCoordinationConfig,
            @JsonProperty("pipeline_shutdown") final PipelineShutdownOption pipelineShutdown) {
        this.authentication = authentication;
        this.circuitBreakerConfig = circuitBreakerConfig;
        this.sourceCoordinationConfig = sourceCoordinationConfig;
        this.pipelineShutdown = pipelineShutdown != null ? pipelineShutdown : DEFAULT_PIPELINE_SHUTDOWN;
        setSsl(ssl);
        this.keyStoreFilePath = keyStoreFilePath != null ? keyStoreFilePath : "";
        this.keyStorePassword = keyStorePassword != null ? keyStorePassword : "";
        this.privateKeyPassword = privateKeyPassword != null ? privateKeyPassword : "";
        this.metricRegistries = metricRegistries != null && !metricRegistries.isEmpty() ? metricRegistries : DEFAULT_METRIC_REGISTRY_TYPE;
        setMetricTags(metricTags);
        setMetricTagFilters(metricTagFilters);
        setServerPort(serverPort);
        this.peerForwarderConfiguration = peerForwarderConfiguration;

        this.processorShutdownTimeout = processorShutdownTimeout != null ? processorShutdownTimeout : DEFAULT_SHUTDOWN_DURATION;
        if (this.processorShutdownTimeout.isNegative()) {
            throw new IllegalArgumentException("processorShutdownTimeout must be non-negative.");
        }

        this.sinkShutdownTimeout = sinkShutdownTimeout != null ? sinkShutdownTimeout : DEFAULT_SHUTDOWN_DURATION;
        if (this.sinkShutdownTimeout.isNegative()) {
            throw new IllegalArgumentException("sinkShutdownTimeout must be non-negative.");
        }
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

    public List<MetricTagFilter> getMetricTagFilters() {
        return metricTagFilters;
    }

    private void setSsl(final Boolean ssl) {
        if (ssl != null) {
            this.ssl = ssl;
        }
    }

    public PluginModel getAuthentication() {
        return authentication;
    }

    public PeerForwarderConfiguration getPeerForwarderConfiguration() {
        return peerForwarderConfiguration;
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
                throw new IllegalArgumentException("metricTags cannot be more than " + MAX_TAGS_NUMBER);
            }
            this.metricTags = metricTags;
        }
    }

    public void setMetricTagFilters(final List<MetricTagFilter> metricTagFilters) {
        if (metricTagFilters != null) {
            metricTagFilters.forEach(
                    metricTagFilter -> {
                        if (metricTagFilter.getTags() != null && metricTagFilter.getTags().size() > MAX_TAGS_NUMBER) {
                            throw new IllegalArgumentException(
                                    String.format("Each metric tag filter may have no more than %s tags.", MAX_TAGS_NUMBER)
                            );
                        }
                    }
            );
            this.metricTagFilters = metricTagFilters;
        }
    }

    public Duration getProcessorShutdownTimeout() {
        return processorShutdownTimeout;
    }

    public Duration getSinkShutdownTimeout() {
        return sinkShutdownTimeout;
    }

    public CircuitBreakerConfig getCircuitBreakerConfig() {
        return circuitBreakerConfig;
    }

    public SourceCoordinationConfig getSourceCoordinationConfig() { return sourceCoordinationConfig; }

    public PipelineShutdownOption getPipelineShutdown() {
        return pipelineShutdown;
    }
}
