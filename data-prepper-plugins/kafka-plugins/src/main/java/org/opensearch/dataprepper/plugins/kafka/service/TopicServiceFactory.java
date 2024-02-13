/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.service;

import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaProducerConfig;

public class TopicServiceFactory {
    public TopicService createTopicService(final KafkaProducerConfig kafkaProducerConfig) {
        return new TopicService(kafkaProducerConfig);
    }
}
