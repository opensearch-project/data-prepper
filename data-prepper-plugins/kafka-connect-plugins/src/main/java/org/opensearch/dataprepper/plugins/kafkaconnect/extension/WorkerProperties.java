/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafkaconnect.extension;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.connect.runtime.WorkerConfig;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class WorkerProperties {
    private static final String KEY_CONVERTER = "org.apache.kafka.connect.json.JsonConverter";
    private static final String KEY_CONVERTER_SCHEMAS_ENABLE = "false";
    private static final String VALUE_CONVERTER_SCHEMAS_ENABLE = "false";
    private static final String VALUE_CONVERTER = "org.apache.kafka.connect.json.JsonConverter";
    private static final Integer OFFSET_STORAGE_PARTITIONS = 25;
    private static final Long OFFSET_FLUSH_INTERVAL_MS = 60000L;
    private static final Long OFFSET_FLUSH_TIMEOUT_MS = 5000L;
    private static final Integer STATUS_STORAGE_PARTITIONS = 5;
    private static final Long HEARTBEAT_INTERVAL_MS = 3000L;
    private static final Long SESSION_TIMEOUT_MS = 30000L;
    private static final String DEFAULT_GROUP_ID = "localGroup";
    private static final String DEFAULT_CLIENT_ID = "localClient";
    private static final String DEFAULT_CONFIG_STORAGE_TOPIC = "config-storage-topic";
    private static final String DEFAULT_OFFSET_STORAGE_TOPIC = "offset-storage-topic";
    private static final String DEFAULT_STATUS_STORAGE_TOPIC = "status-storage-topic";
    private final Integer offsetStorageReplicationFactor = -1;
    private final Integer configStorageReplicationFactor = -1;
    private final Integer statusStorageReplicationFactor = -1;
    private String keyConverter = KEY_CONVERTER;
    private String keyConverterSchemasEnable = KEY_CONVERTER_SCHEMAS_ENABLE;
    private String valueConverter = VALUE_CONVERTER;
    private String valueConverterSchemasEnable = VALUE_CONVERTER_SCHEMAS_ENABLE;

    @JsonProperty("group_id")
    private String groupId = DEFAULT_GROUP_ID;
    @JsonProperty("config_storage_topic")
    private String configStorageTopic = DEFAULT_CONFIG_STORAGE_TOPIC;
    @JsonProperty("offset_storage_topic")
    private String offsetStorageTopic = DEFAULT_OFFSET_STORAGE_TOPIC;
    @JsonProperty("status_storage_topic")
    private String statusStorageTopic = DEFAULT_STATUS_STORAGE_TOPIC;
    @JsonProperty("client_id")
    private String clientId = DEFAULT_CLIENT_ID;
    @JsonProperty("offset_storage_partitions")
    private Integer offsetStoragePartitions = OFFSET_STORAGE_PARTITIONS;
    @JsonProperty("offset_flush_interval")
    private Duration offsetFlushInterval = Duration.ofMillis(OFFSET_FLUSH_INTERVAL_MS);
    @JsonProperty("offset_flush_timeout")
    private Duration offsetFlushTimeout = Duration.ofMillis(OFFSET_FLUSH_TIMEOUT_MS);
    @JsonProperty("status_storage_partitions")
    private Integer statusStoragePartitions = STATUS_STORAGE_PARTITIONS;
    @JsonProperty("heartbeat_interval")
    private Duration heartBeatInterval = Duration.ofMillis(HEARTBEAT_INTERVAL_MS);
    @JsonProperty("session_timeout")
    private Duration sessionTimeout = Duration.ofMillis(SESSION_TIMEOUT_MS);
    private String keyConverterSchemaRegistryUrl;
    private String valueConverterSchemaRegistryUrl;
    private String bootstrapServers;
    private Properties authProperties;

    public String getKeyConverter() {
        return keyConverter;
    }

    public String getKeyConverterSchemasEnable() {
        return keyConverterSchemasEnable;
    }

    public String getKeyConverterSchemaRegistryUrl() {
        return keyConverterSchemaRegistryUrl;
    }

    public String getValueConverter() {
        return valueConverter;
    }

    public String getValueConverterSchemasEnable() {
        return valueConverterSchemasEnable;
    }

    public String getValueConverterSchemaRegistryUrl() {
        return valueConverterSchemaRegistryUrl;
    }

    public Integer getOffsetStoragePartitions() {
        return offsetStoragePartitions;
    }

    public Long getOffsetFlushInterval() {
        return offsetFlushInterval.toMillis();
    }

    public Long getOffsetFlushTimeout() {
        return offsetFlushTimeout.toMillis();
    }

    public Integer getStatusStoragePartitions() {
        return statusStoragePartitions;
    }

    public Long getHeartBeatInterval() {
        return heartBeatInterval.toMillis();
    }

    public Long getSessionTimeout() {
        return sessionTimeout.toMillis();
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(final String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getClientId() {
        return clientId;
    }

    public String getConfigStorageTopic() {
        return configStorageTopic;
    }

    public Integer getConfigStorageReplicationFactor() {
        return configStorageReplicationFactor;
    }

    public String getOffsetStorageTopic() {
        return offsetStorageTopic;
    }

    public Integer getOffsetStorageReplicationFactor() {
        return offsetStorageReplicationFactor;
    }

    public String getStatusStorageTopic() {
        return statusStorageTopic;
    }

    public Integer getStatusStorageReplicationFactor() {
        return statusStorageReplicationFactor;
    }

    public void setAuthProperties(Properties authProperties) {
        this.authProperties = authProperties;
    }

    public Map<String, String> buildKafkaConnectPropertyMap() {
        final String producerPrefix = "producer.";
        Map<String, String> workerProps = new HashMap<>();
        if (authProperties != null) {
            authProperties.forEach((k, v) -> {
                if (k == WorkerConfig.BOOTSTRAP_SERVERS_CONFIG) {
                    this.setBootstrapServers(v.toString());
                    return;
                }
                if (v instanceof Class) {
                    workerProps.put(k.toString(), ((Class<?>) v).getName());
                    workerProps.put(producerPrefix + k, ((Class<?>) v).getName());
                    return;
                }
                workerProps.put(k.toString(), v.toString());
                workerProps.put(producerPrefix + k, v.toString());
            });
        }
        workerProps.put("bootstrap.servers", this.getBootstrapServers());
        workerProps.put("group.id", this.getGroupId());
        workerProps.put("client.id", this.getClientId());
        workerProps.put("offset.storage.topic", this.getOffsetStorageTopic());
        workerProps.put("offset.storage.replication.factor", this.getOffsetStorageReplicationFactor().toString());
        workerProps.put("config.storage.topic", this.getConfigStorageTopic());
        workerProps.put("config.storage.replication.factor", this.getConfigStorageReplicationFactor().toString());
        workerProps.put("status.storage.topic", this.getStatusStorageTopic());
        workerProps.put("status.storage.replication.factor", this.getStatusStorageReplicationFactor().toString());
        workerProps.put("key.converter", this.getKeyConverter());
        workerProps.put("key.converter.schemas.enable", this.getKeyConverterSchemasEnable());
        if (this.getKeyConverterSchemaRegistryUrl() != null) {
            workerProps.put("key.converter.schema.registry.url", this.getKeyConverterSchemaRegistryUrl());
        }
        workerProps.put("value.converter", this.getValueConverter());
        workerProps.put("value.converter.schemas.enable", this.getValueConverterSchemasEnable());
        if (this.getValueConverterSchemaRegistryUrl() != null) {
            workerProps.put("value.converter.schema.registry.url", this.getValueConverterSchemaRegistryUrl());
        }
        workerProps.put("offset.storage.partitions", this.getOffsetStoragePartitions().toString());
        workerProps.put("offset.flush.interval.ms", this.getOffsetFlushInterval().toString());
        workerProps.put("offset.flush.timeout.ms", this.getOffsetFlushTimeout().toString());
        workerProps.put("status.storage.partitions", this.getStatusStoragePartitions().toString());
        workerProps.put("heartbeat.interval.ms", this.getHeartBeatInterval().toString());
        workerProps.put("session.timeout.ms", this.getSessionTimeout().toString());
        return workerProps;
    }
}

