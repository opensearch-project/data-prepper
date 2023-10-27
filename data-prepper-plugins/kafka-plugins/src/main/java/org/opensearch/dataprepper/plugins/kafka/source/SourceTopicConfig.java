/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Size;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.kafka.configuration.CommonTopicConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConsumerConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaKeyMode;
import org.opensearch.dataprepper.plugins.kafka.configuration.KmsConfig;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;

import java.time.Duration;

class SourceTopicConfig extends CommonTopicConfig implements TopicConsumerConfig {
    static final boolean DEFAULT_AUTO_COMMIT = false;
    static final Duration DEFAULT_COMMIT_INTERVAL = Duration.ofSeconds(5);
    static final String DEFAULT_FETCH_MAX_BYTES = "50mb";
    static final Integer DEFAULT_FETCH_MAX_WAIT = 500;
    static final String DEFAULT_FETCH_MIN_BYTES = "1b";
    static final String DEFAULT_MAX_PARTITION_FETCH_BYTES = "1mb";


    @JsonProperty("serde_format")
    private MessageFormat serdeFormat = MessageFormat.PLAINTEXT;

    @JsonProperty("commit_interval")
    @Valid
    @Size(min = 1)
    private Duration commitInterval = DEFAULT_COMMIT_INTERVAL;

    @JsonProperty("key_mode")
    private KafkaKeyMode kafkaKeyMode = KafkaKeyMode.INCLUDE_AS_FIELD;

    @JsonProperty("auto_commit")
    private Boolean autoCommit = DEFAULT_AUTO_COMMIT;

    @JsonProperty("max_partition_fetch_bytes")
    private String maxPartitionFetchBytes = DEFAULT_MAX_PARTITION_FETCH_BYTES;

    @JsonProperty("fetch_max_bytes")
    private String fetchMaxBytes = DEFAULT_FETCH_MAX_BYTES;

    @JsonProperty("fetch_max_wait")
    @Valid
    @Size(min = 1)
    private Integer fetchMaxWait = DEFAULT_FETCH_MAX_WAIT;

    @JsonProperty("fetch_min_bytes")
    private String fetchMinBytes = DEFAULT_FETCH_MIN_BYTES;


    @Override
    public String getEncryptionKey() {
        return null;
    }

    @Override
    public KmsConfig getKmsConfig() {
        return null;
    }


    @Override
    public Duration getCommitInterval() {
        return commitInterval;
    }


    @Override
    public KafkaKeyMode getKafkaKeyMode() {
        return kafkaKeyMode;
    }

    @Override
    public MessageFormat getSerdeFormat() {
        return serdeFormat;
    }

    @Override
    public Boolean getAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(Boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    @Override
    public long getFetchMaxBytes() {
        long value = ByteCount.parse(fetchMaxBytes).getBytes();
        if (value < 1 || value > 50*1024*1024) {
            throw new RuntimeException("Invalid Fetch Max Bytes");
        }
        return value;
    }

    @Override
    public Integer getFetchMaxWait() {
        return fetchMaxWait;
    }

    @Override
    public long getFetchMinBytes() {
        long value = ByteCount.parse(fetchMinBytes).getBytes();
        if (value < 1) {
            throw new RuntimeException("Invalid Fetch Min Bytes");
        }
        return value;
    }

    @Override
    public long getMaxPartitionFetchBytes() {
        return ByteCount.parse(maxPartitionFetchBytes).getBytes();
    }
}
