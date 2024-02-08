package org.opensearch.dataprepper.plugins.kafka.common.key;

import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;

import java.util.function.Function;

interface InnerKeyProvider extends Function<TopicConfig, byte[]> {
    boolean supportsConfiguration(TopicConfig topicConfig);

    /**
     * Returns true if this {@link InnerKeyProvider} expects an encrypted
     * data key. Returns false if the encryption key is in plain-text
     * in the pipeline configuration.
     *
     * @return True if the key is encrypted.
     */
    boolean isKeyEncrypted();
}
