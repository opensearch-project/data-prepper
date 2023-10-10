package org.opensearch.dataprepper.plugins.kafka.common.key;

import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DecryptResponse;

import java.util.function.Supplier;

class KmsKeyProvider implements InnerKeyProvider {
    private final Supplier<AwsCredentialsProvider> awsCredentialsProviderSupplier;

    public KmsKeyProvider(Supplier<AwsCredentialsProvider> awsCredentialsProviderSupplier) {
        this.awsCredentialsProviderSupplier = awsCredentialsProviderSupplier;
    }

    @Override
    public boolean supportsConfiguration(TopicConfig topicConfig) {
        return topicConfig.getKmsKeyId() != null;
    }

    @Override
    public byte[] apply(TopicConfig topicConfig) {
        String kmsKeyId = topicConfig.getKmsKeyId();

        AwsCredentialsProvider awsCredentialsProvider = awsCredentialsProviderSupplier.get();

        KmsClient kmsClient = KmsClient.builder()
                .credentialsProvider(awsCredentialsProvider)
                .build();

        DecryptResponse decryptResponse = kmsClient.decrypt(builder -> builder.keyId(kmsKeyId));

        return decryptResponse.plaintext().asByteArray();
    }
}
