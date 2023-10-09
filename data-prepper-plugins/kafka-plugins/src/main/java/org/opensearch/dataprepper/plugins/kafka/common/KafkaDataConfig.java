package org.opensearch.dataprepper.plugins.kafka.common;

import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;

/**
 * An interface representing important data for how the data going to or coming from
 * Kafka should be represented.
 */
public interface KafkaDataConfig {
    MessageFormat getSerdeFormat();
}
