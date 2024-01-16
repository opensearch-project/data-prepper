/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.kafka.service;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaProducerConfig;
import org.opensearch.dataprepper.plugins.kafka.util.SinkPropertyConfigurer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TopicService {
    private static final Logger LOG = LoggerFactory.getLogger(TopicService.class);
    private final AdminClient adminClient;


    public TopicService(final KafkaProducerConfig kafkaProducerConfig) {
        this.adminClient = AdminClient.create(SinkPropertyConfigurer.getPropertiesForAdminClient(kafkaProducerConfig));
    }

    public void createTopic(final String topicName, final Integer numberOfPartitions, final Short replicationFactor, final Long maxMessageBytes) {
        try {
            final NewTopic newTopic = new NewTopic(topicName, numberOfPartitions, replicationFactor);
            if (maxMessageBytes != null) {
                Map<String, String> configOptions = new HashMap<>();
                configOptions.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, Long.toString(maxMessageBytes));
                newTopic.configs(configOptions);
            }

            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
            LOG.info(topicName + " created successfully");

        } catch (Exception e) {
            LOG.error("Caught exception creating topic with name: {}", topicName, e);
        }
    }

    public void closeAdminClient() {
        adminClient.close();
    }
}
