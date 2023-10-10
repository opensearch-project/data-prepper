package org.opensearch.dataprepper.plugins.kafka.common.key;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.kafka.configuration.AwsConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.KmsClientBuilder;
import software.amazon.awssdk.services.kms.model.DecryptResponse;

import java.util.UUID;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KmsKeyProviderTest {
    @Mock
    private AwsCredentialsProvider awsCredentialsProvider;
    @Mock
    private TopicConfig topicConfig;
    @Mock
    private AwsConfig awsConfig;

    private KmsKeyProvider createObjectUnderTest() {
        return new KmsKeyProvider(() -> awsCredentialsProvider);
    }

    @Test
    void supportsConfiguration_returns_true_if_kmsKeyId_is_present() {
        when(topicConfig.getKmsKeyId()).thenReturn(UUID.randomUUID().toString());
        assertThat(createObjectUnderTest().supportsConfiguration(topicConfig), equalTo(true));
    }

    @Test
    void supportsConfiguration_returns_false_if_kmsKeyId_is_null() {
        assertThat(createObjectUnderTest().supportsConfiguration(topicConfig), equalTo(false));
    }

    @Test
    void apply_returns_plaintext_from_decrypt_request() {
        KmsClient kmsClient = mock(KmsClient.class);
        DecryptResponse decryptResponse = mock(DecryptResponse.class);
        byte[] decryptedBytes = UUID.randomUUID().toString().getBytes();
        when(decryptResponse.plaintext()).thenReturn(SdkBytes.fromByteArray(decryptedBytes));
        when(kmsClient.decrypt(any(Consumer.class))).thenReturn(decryptResponse);

        KmsClientBuilder kmsClientBuilder = mock(KmsClientBuilder.class);
        when(kmsClientBuilder.credentialsProvider(awsCredentialsProvider)).thenReturn(kmsClientBuilder);
        when(kmsClientBuilder.build()).thenReturn(kmsClient);

        byte[] actualBytes;
        try(MockedStatic<KmsClient> kmsClientMockedStatic = mockStatic(KmsClient.class)) {
            kmsClientMockedStatic.when(() -> KmsClient.builder()).thenReturn(kmsClientBuilder);
            actualBytes = createObjectUnderTest().apply(topicConfig);
        }

        assertThat(actualBytes, equalTo(decryptedBytes));
    }
}