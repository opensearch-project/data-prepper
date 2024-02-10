package org.opensearch.dataprepper.plugins.kafka.common.key;

import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;

import java.util.Base64;

class UnencryptedKeyProvider implements InnerKeyProvider {
    @Override
    public boolean supportsConfiguration(final TopicConfig topicConfig) {
        return true;
    }

    @Override
    public boolean isKeyEncrypted() {
        return false;
    }

    @Override
    public byte[] apply(final TopicConfig topicConfig) {
        return Base64.getDecoder().decode(topicConfig.getEncryptionKey());
    }
}
