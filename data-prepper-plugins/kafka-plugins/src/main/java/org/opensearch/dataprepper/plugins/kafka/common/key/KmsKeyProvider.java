package org.opensearch.dataprepper.plugins.kafka.common.key;

import org.opensearch.dataprepper.plugins.kafka.common.aws.AwsContext;
import org.opensearch.dataprepper.plugins.kafka.configuration.KmsConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DecryptResponse;

import java.util.Base64;

class KmsKeyProvider implements InnerKeyProvider {
    private final AwsContext awsContext;

    public KmsKeyProvider(final AwsContext awsContext) {
        this.awsContext = awsContext;
    }

    @Override
    public boolean supportsConfiguration(final TopicConfig topicConfig) {
        return topicConfig.getKmsConfig() != null && topicConfig.getKmsConfig().getKeyId() != null;
    }

    @Override
    public boolean isKeyEncrypted() {
        return true;
    }

    @Override
    public byte[] apply(final TopicConfig topicConfig) {
        final KmsConfig kmsConfig = topicConfig.getKmsConfig();
        final String kmsKeyId = kmsConfig.getKeyId();

        final AwsCredentialsProvider awsCredentialsProvider = awsContext.getOrDefault(kmsConfig);

        final KmsClient kmsClient = KmsClient.builder()
                .credentialsProvider(awsCredentialsProvider)
                .region(awsContext.getRegionOrDefault(kmsConfig))
                .build();

        final byte[] decodedEncryptionKey = Base64.getDecoder().decode(topicConfig.getEncryptionKey());
        final DecryptResponse decryptResponse = kmsClient.decrypt(builder -> builder
                .keyId(kmsKeyId)
                .ciphertextBlob(SdkBytes.fromByteArray(decodedEncryptionKey))
                .encryptionContext(kmsConfig.getEncryptionContext())
        );

        return decryptResponse.plaintext().asByteArray();
    }
}
