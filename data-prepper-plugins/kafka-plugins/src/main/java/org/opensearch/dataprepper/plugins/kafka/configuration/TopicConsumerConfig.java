/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import java.time.Duration;

/**
 * An extension of the {@link TopicConfig} specifically for
 * consumers from Kafka topics.
 */
public interface TopicConsumerConfig extends TopicConfig {
    KafkaKeyMode getKafkaKeyMode();

    String getGroupId();

    Boolean getAutoCommit();

    String getAutoOffsetReset();

    Duration getCommitInterval();

    long getMaxPartitionFetchBytes();

    long getFetchMaxBytes();

    Integer getFetchMaxWait();

    long getFetchMinBytes();

    Duration getThreadWaitingTime();

    Duration getSessionTimeOut();

    Duration getHeartBeatInterval();

    Duration getMaxPollInterval();

    Integer getConsumerMaxPollRecords();

    Integer getWorkers();
}
