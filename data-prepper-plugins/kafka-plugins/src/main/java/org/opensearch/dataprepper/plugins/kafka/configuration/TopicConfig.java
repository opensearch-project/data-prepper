/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;

import java.time.Duration;

/**
 * Represents a topic configuration to use throughout the code. See the
 * {@link ConsumerTopicConfig} and {@link ProducerTopicConfig} for configurations
 * which are specific for those use-cases.
 */
public interface TopicConfig {
    String getGroupId();

    MessageFormat getSerdeFormat();

    String getEncryptionKey();

    KmsConfig getKmsConfig();

    Boolean getAutoCommit();

    Duration getSessionTimeOut();

    String getAutoOffsetReset();

    Duration getThreadWaitingTime();

    long getMaxPartitionFetchBytes();

    long getFetchMaxBytes();

    Integer getFetchMaxWait();

    long getFetchMinBytes();

    Duration getRetryBackoff();

    Duration getReconnectBackoff();

    Duration getMaxPollInterval();

    Integer getConsumerMaxPollRecords();

    Integer getWorkers();

    Duration getHeartBeatInterval();

    String getName();
}
