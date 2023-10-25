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
public interface ConsumerTopicConfig extends TopicConfig {
    KafkaKeyMode getKafkaKeyMode();

    Duration getCommitInterval();
}
