/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.buffer;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Size;
import org.opensearch.dataprepper.plugins.kafka.configuration.CommonTopicConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.ConsumerTopicConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaKeyMode;
import org.opensearch.dataprepper.plugins.kafka.configuration.KmsConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.ProducerTopicConfig;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;

import java.time.Duration;

class BufferTopicConfig extends CommonTopicConfig implements ProducerTopicConfig, ConsumerTopicConfig {
    static final Duration DEFAULT_COMMIT_INTERVAL = Duration.ofSeconds(5);
    private static final Integer DEFAULT_NUM_OF_PARTITIONS = 1;
    private static final Short DEFAULT_REPLICATION_FACTOR = 1;
    private static final Long DEFAULT_RETENTION_PERIOD = 604800000L;


    @JsonProperty("encryption_key")
    private String encryptionKey;

    @JsonProperty("kms")
    private KmsConfig kmsConfig;

    @JsonProperty("commit_interval")
    @Valid
    @Size(min = 1)
    private Duration commitInterval = DEFAULT_COMMIT_INTERVAL;

    @JsonProperty("number_of_partitions")
    private Integer numberOfPartitions = DEFAULT_NUM_OF_PARTITIONS;

    @JsonProperty("replication_factor")
    private Short replicationFactor = DEFAULT_REPLICATION_FACTOR;

    @JsonProperty("retention_period")
    private Long retentionPeriod = DEFAULT_RETENTION_PERIOD;

    @JsonProperty("is_topic_create")
    private Boolean isTopicCreate = Boolean.FALSE;


    @Override
    public MessageFormat getSerdeFormat() {
        return MessageFormat.BYTES;
    }

    @Override
    public String getEncryptionKey() {
        return encryptionKey;
    }

    @Override
    public KmsConfig getKmsConfig() {
        return kmsConfig;
    }

    @Override
    public KafkaKeyMode getKafkaKeyMode() {
        return KafkaKeyMode.DISCARD;
    }

    @Override
    public Duration getCommitInterval() {
        return commitInterval;
    }


    @Override
    public Integer getNumberOfPartitions() {
        return numberOfPartitions;
    }

    @Override
    public Short getReplicationFactor() {
        return replicationFactor;
    }

    @Override
    public Long getRetentionPeriod() {
        return retentionPeriod;
    }

    @Override
    public Boolean isCreate() {
        return isTopicCreate;
    }
}
