package org.opensearch.dataprepper.plugins.kafka.common;

import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;

public class PlaintextKafkaDataConfig implements KafkaDataConfig {
    /**
     * Gets similar {@link KafkaDataConfig} as the given one, but uses {@link MessageFormat#PLAINTEXT} for
     * the serialization/deserialization format.
     *
     * @param dataConfig The configuration to replicate.
     * @return A {@link KafkaDataConfig} with the PLAINTEXT message format.
     */
    public static KafkaDataConfig plaintextDataConfig(final KafkaDataConfig dataConfig) {
        return new PlaintextKafkaDataConfig();
    }

    @Override
    public MessageFormat getSerdeFormat() {
        return MessageFormat.PLAINTEXT;
    }
}
