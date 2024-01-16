/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import java.util.List;

public interface KafkaConsumerConfig extends KafkaConnectionConfig {
    String getClientDnsLookup();

    boolean getAcknowledgementsEnabled();
    
    SchemaConfig getSchemaConfig();

    List<? extends TopicConfig> getTopics();
}
