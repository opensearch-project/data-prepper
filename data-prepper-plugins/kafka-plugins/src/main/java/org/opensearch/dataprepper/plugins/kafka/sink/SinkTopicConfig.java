/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.sink;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.opensearch.dataprepper.plugins.kafka.configuration.CommonTopicConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KmsConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicProducerConfig;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;

public class SinkTopicConfig extends CommonTopicConfig implements TopicProducerConfig {
    private static final Integer DEFAULT_NUM_OF_PARTITIONS = 1;
    private static final Short DEFAULT_REPLICATION_FACTOR = 1;
    private static final Long DEFAULT_RETENTION_PERIOD = 604800000L;

    @JsonProperty("serde_format")
    private MessageFormat serdeFormat = MessageFormat.PLAINTEXT;

    @JsonProperty("number_of_partitions")
    private Integer numberOfPartitions = DEFAULT_NUM_OF_PARTITIONS;

    @JsonProperty("replication_factor")
    private Short replicationFactor = DEFAULT_REPLICATION_FACTOR;

    @JsonProperty("retention_period")
    private Long retentionPeriod = DEFAULT_RETENTION_PERIOD;

    @JsonProperty("create_topic")
    private boolean isCreateTopic = false;

    @Override
    public MessageFormat getSerdeFormat() {
        return serdeFormat;
    }

    @Override
    public String getEncryptionKey() {
        return null;
    }

    @Override
    public KmsConfig getKmsConfig() {
        return null;
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
    public boolean isCreateTopic() {
        return isCreateTopic;
    }
}
