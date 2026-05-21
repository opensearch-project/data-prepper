/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.iceberg;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.model.configuration.PluginModel;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Configuration for the Iceberg sink plugin.
 */
public class IcebergSinkConfig {

    static final Duration DEFAULT_COMMIT_INTERVAL = Duration.ofMinutes(5);
    static final Duration DEFAULT_FLUSH_INTERVAL = Duration.ofMinutes(5);

    @JsonProperty("catalog")
    @NotNull
    @NotEmpty
    private Map<String, String> catalog;

    @JsonProperty("table_identifier")
    @NotNull
    private String tableIdentifier;

    @JsonProperty("operation")
    private String operation;

    @JsonProperty("identifier_columns")
    private List<String> identifierColumns;

    @JsonProperty("commit_interval")
    private Duration commitInterval = DEFAULT_COMMIT_INTERVAL;

    @JsonProperty("flush_interval")
    private Duration flushInterval = DEFAULT_FLUSH_INTERVAL;

    @JsonProperty("auto_create")
    private boolean autoCreate = false;

    @JsonProperty("schema_evolution")
    private boolean schemaEvolution = false;

    @JsonProperty("schema")
    private SchemaConfig schemaConfig;

    @JsonProperty("dlq")
    private PluginModel dlq;

    @JsonProperty("table_properties")
    private Map<String, String> tableProperties;

    @JsonProperty("table_location")
    private String tableLocation;

    @JsonProperty("ack_poll_interval")
    private Duration ackPollInterval = Duration.ofSeconds(5);

    public Map<String, String> getCatalog() {
        return catalog;
    }

    public String getTableIdentifier() {
        return tableIdentifier;
    }

    public String getOperation() {
        return operation;
    }

    public List<String> getIdentifierColumns() {
        return identifierColumns != null ? identifierColumns : Collections.emptyList();
    }

    public Duration getCommitInterval() {
        return commitInterval;
    }

    public Duration getFlushInterval() {
        return flushInterval;
    }

    public boolean isAutoCreate() {
        return autoCreate;
    }

    public boolean isSchemaEvolution() {
        return schemaEvolution;
    }

    public SchemaConfig getSchemaConfig() {
        return schemaConfig;
    }

    public PluginModel getDlq() {
        return dlq;
    }

    public Map<String, String> getTableProperties() {
        return tableProperties != null ? tableProperties : Collections.emptyMap();
    }

    public String getTableLocation() {
        return tableLocation;
    }

    public Duration getAckPollInterval() {
        return ackPollInterval;
    }
}
