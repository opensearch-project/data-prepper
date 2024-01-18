package org.opensearch.dataprepper.plugins.kafka.common.key;

import org.opensearch.dataprepper.plugins.kafka.common.aws.AwsContext;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;

import java.util.List;
import java.util.function.Supplier;

public class KeyFactory {
    private final List<InnerKeyProvider> orderedKeyProviders;

    public KeyFactory(final AwsContext awsContext) {
        this(List.of(
                new KmsKeyProvider(awsContext),
                new UnencryptedKeyProvider()
        ));
    }

    KeyFactory(final List<InnerKeyProvider> orderedKeyProviders) {
        this.orderedKeyProviders = orderedKeyProviders;
    }

    public Supplier<byte[]> getKeySupplier(final TopicConfig topicConfig) {
        if (topicConfig.getEncryptionKey() == null)
            return null;

        final InnerKeyProvider keyProvider = getInnerKeyProvider(topicConfig);

        return () -> keyProvider.apply(topicConfig);
    }

    public String getEncryptedDataKey(final TopicConfig topicConfig) {
        if (topicConfig.getEncryptionKey() == null)
            return null;

        final InnerKeyProvider keyProvider = getInnerKeyProvider(topicConfig);

        if(keyProvider.isKeyEncrypted())
            return topicConfig.getEncryptionKey();

        return null;
    }

    private InnerKeyProvider getInnerKeyProvider(final TopicConfig topicConfig) {
        return orderedKeyProviders
                .stream()
                .filter(innerKeyProvider -> innerKeyProvider.supportsConfiguration(topicConfig))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Unable to find an inner key provider. This is a programming error - UnencryptedKeyProvider should always work."));
    }
}
