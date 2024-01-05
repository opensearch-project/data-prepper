/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import java.util.Optional;

/**
 * An extension of the {@link TopicConfig} specifically for
 * producers to Kafka topics.
 */
public interface TopicProducerConfig extends TopicConfig {
    Integer getNumberOfPartitions();
    Short getReplicationFactor();
    Long getRetentionPeriod();
    Optional<Long> getMaxMessageBytes();
    boolean isCreateTopic();
}
