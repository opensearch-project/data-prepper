package org.opensearch.dataprepper.plugins.kafka.common.key;

import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;

import java.util.function.Function;

interface InnerKeyProvider extends Function<TopicConfig, byte[]> {
    boolean supportsConfiguration(TopicConfig topicConfig);
}
