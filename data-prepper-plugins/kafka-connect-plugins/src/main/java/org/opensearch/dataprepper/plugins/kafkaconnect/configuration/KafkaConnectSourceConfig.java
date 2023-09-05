/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafkaconnect.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.AwsConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionConfig;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaClusterAuthConfig;
import org.opensearch.dataprepper.plugins.kafkaconnect.util.Connector;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaConnectSourceConfig implements KafkaClusterAuthConfig {
    private static final long CONNECTOR_TIMEOUT_MS = 30000L; // 30 seconds
    private static final long CONNECT_TIMEOUT_MS = 60000L; // 60 seconds
    @JsonProperty("mysql")
    private MySQLConfig mysql;

    @JsonProperty("postgresql")
    private PostgreSQLConfig postgresql;

    @JsonProperty("mongodb")
    private MongoDBConfig mongodb;

    @JsonProperty("worker_properties")
    private WorkerProperties workerProperties = new WorkerProperties();

    @JsonProperty("connect_timeout_ms")
    private Long connectTimeoutMs = CONNECT_TIMEOUT_MS;

    @JsonProperty("connector_timeout_ms")
    private Long connectorTimeoutMs = CONNECTOR_TIMEOUT_MS;

    private String bootstrapServers;
    private AuthConfig authConfig;
    private EncryptionConfig encryptionConfig;
    private AwsConfig awsConfig;

    public Long getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public Long getConnectorTimeoutMs() {
        return connectorTimeoutMs;
    }

    public void setBootstrapServers(final String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        this.workerProperties.setBootstrapServers(bootstrapServers);
        if (this.mysql != null) {
            this.mysql.setBootstrapServers(bootstrapServers);
        }
    }

    public void setAuthProperty(final Properties authProperties) {
        this.workerProperties.setAuthProperty(authProperties);
        if (this.mysql != null) {
            this.mysql.setAuthProperty(authProperties);
        }
    }

    public void setAuthConfig(AuthConfig authConfig) {
        this.authConfig = authConfig;
    }

    public void setAwsConfig(AwsConfig awsConfig) {
        this.awsConfig = awsConfig;
    }

    public void setEncryptionConfig(EncryptionConfig encryptionConfig) {
        this.encryptionConfig = encryptionConfig;
    }

    public WorkerProperties getWorkerProperties() {
        return workerProperties;
    }

    public List<Connector> getConnectors() {
        List<Connector> connectors = new ArrayList<>();
        if (this.mysql != null) {
            connectors.addAll(this.mysql.buildConnectors());
        }
        if (this.postgresql != null) {
            connectors.addAll(this.postgresql.buildConnectors());
        }
        if (this.mongodb != null) {
            connectors.addAll(this.mongodb.buildConnectors());
        }
        return connectors;
    }

    @Override
    public AwsConfig getAwsConfig() {
        return awsConfig;
    }

    @Override
    public AuthConfig getAuthConfig() {
        return authConfig;
    }

    @Override
    public EncryptionConfig getEncryptionConfig() {
        return encryptionConfig;
    }

    @Override
    public String getBootStrapServers() {
        return bootstrapServers;
    }
}
