/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

/**
 * An extension of the {@link TopicConfig} specifically for
 * producers to Kafka topics.
 */
public interface ProducerTopicConfig extends TopicConfig {
    Integer getNumberOfPartitions();
    Short getReplicationFactor();
    Long getRetentionPeriod();
    Boolean isCreate();
}
