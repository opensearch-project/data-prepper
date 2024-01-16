/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;

import java.time.Duration;

/**
 * Represents a topic configuration to use throughout the code. See the
 * {@link TopicConsumerConfig} and {@link TopicProducerConfig} for configurations
 * which are specific for those use-cases.
 */
public interface TopicConfig {
    String getName();

    MessageFormat getSerdeFormat();

    String getEncryptionKey();

    KmsConfig getKmsConfig();

    Duration getRetryBackoff();

    Duration getReconnectBackoff();
}
