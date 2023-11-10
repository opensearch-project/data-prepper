/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.common;

import org.opensearch.dataprepper.plugins.kafka.common.key.KeyFactory;
import org.opensearch.dataprepper.plugins.kafka.configuration.CommonTopicConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;

import java.util.function.Supplier;

/**
 * Adapts a {@link CommonTopicConfig} to a {@link KafkaDataConfig}.
 */
public class KafkaDataConfigAdapter implements KafkaDataConfig {
    private final KeyFactory keyFactory;
    private final TopicConfig topicConfig;

    public KafkaDataConfigAdapter(KeyFactory keyFactory, TopicConfig topicConfig) {
        this.keyFactory = keyFactory;
        this.topicConfig = topicConfig;
    }

    @Override
    public MessageFormat getSerdeFormat() {
        return topicConfig.getSerdeFormat();
    }

    @Override
    public Supplier<byte[]> getEncryptionKeySupplier() {
        if(topicConfig.getEncryptionKey() == null)
            return null;
        return keyFactory.getKeySupplier(topicConfig);
    }
}
