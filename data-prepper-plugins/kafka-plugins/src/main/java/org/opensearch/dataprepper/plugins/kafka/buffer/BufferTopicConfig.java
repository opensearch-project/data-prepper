/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.buffer;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Size;
import org.opensearch.dataprepper.model.types.ByteCount;
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
    static final boolean DEFAULT_AUTO_COMMIT = false;
    static final ByteCount DEFAULT_FETCH_MAX_BYTES = ByteCount.parse("50mb");
    static final Duration DEFAULT_FETCH_MAX_WAIT = Duration.ofMillis(500);
    static final ByteCount DEFAULT_FETCH_MIN_BYTES = ByteCount.parse("1b");
    static final ByteCount DEFAULT_MAX_PARTITION_FETCH_BYTES = ByteCount.parse("1mb");


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

    @JsonProperty("auto_commit")
    private Boolean autoCommit = DEFAULT_AUTO_COMMIT;

    @JsonProperty("max_partition_fetch_bytes")
    private ByteCount maxPartitionFetchBytes = DEFAULT_MAX_PARTITION_FETCH_BYTES;

    @JsonProperty("fetch_max_bytes")
    private ByteCount fetchMaxBytes = DEFAULT_FETCH_MAX_BYTES;

    @JsonProperty("fetch_max_wait")
    @Valid
    private Duration fetchMaxWait = DEFAULT_FETCH_MAX_WAIT;

    @JsonProperty("fetch_min_bytes")
    private ByteCount fetchMinBytes = DEFAULT_FETCH_MIN_BYTES;

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

    @Override
    public Boolean getAutoCommit() {
        return autoCommit;
    }

    @Override
    public long getFetchMaxBytes() {
        long value = fetchMaxBytes.getBytes();
        if (value < 1 || value > 50*1024*1024) {
            throw new RuntimeException("Invalid Fetch Max Bytes");
        }
        return value;
    }

    @Override
    public Integer getFetchMaxWait() {
        return Math.toIntExact(fetchMaxWait.toMillis());
    }

    @Override
    public long getFetchMinBytes() {
        long value = fetchMinBytes.getBytes();
        if (value < 1) {
            throw new RuntimeException("Invalid Fetch Min Bytes");
        }
        return value;
    }

    @Override
    public long getMaxPartitionFetchBytes() {
        return maxPartitionFetchBytes.getBytes();
    }
}
