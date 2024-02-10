package org.opensearch.dataprepper.plugins.kafka.common;

import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;

import java.util.function.Supplier;

public class PlaintextKafkaDataConfig implements KafkaDataConfig {
    private final KafkaDataConfig dataConfig;

    private PlaintextKafkaDataConfig(final KafkaDataConfig dataConfig) {
        this.dataConfig = dataConfig;
    }

    /**
     * Gets similar {@link KafkaDataConfig} as the given one, but uses {@link MessageFormat#PLAINTEXT} for
     * the serialization/deserialization format.
     *
     * @param dataConfig The configuration to replicate.
     * @return A {@link KafkaDataConfig} with the PLAINTEXT message format.
     */
    public static KafkaDataConfig plaintextDataConfig(final KafkaDataConfig dataConfig) {
        return new PlaintextKafkaDataConfig(dataConfig);
    }

    @Override
    public MessageFormat getSerdeFormat() {
        return MessageFormat.PLAINTEXT;
    }

    @Override
    public Supplier<byte[]> getEncryptionKeySupplier() {
        return dataConfig.getEncryptionKeySupplier();
    }

    @Override
    public String getEncryptedDataKey() {
        return dataConfig.getEncryptedDataKey();
    }
}
