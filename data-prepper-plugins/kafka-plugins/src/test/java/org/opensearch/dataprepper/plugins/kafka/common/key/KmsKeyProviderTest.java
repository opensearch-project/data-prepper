package org.opensearch.dataprepper.plugins.kafka.common.key;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.kafka.common.aws.AwsContext;
import org.opensearch.dataprepper.plugins.kafka.configuration.KmsConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.KmsClientBuilder;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;

import java.util.Base64;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KmsKeyProviderTest {
    @Mock
    private AwsContext awsContext;
    @Mock
    private AwsCredentialsProvider awsCredentialsProvider;
    @Mock
    private TopicConfig topicConfig;
    @Mock
    private KmsConfig kmsConfig;

    private KmsKeyProvider createObjectUnderTest() {
        return new KmsKeyProvider(awsContext);
    }

    @Test
    void supportsConfiguration_returns_false_if_kms_config_is_null() {
        assertThat(createObjectUnderTest().supportsConfiguration(topicConfig), equalTo(false));
    }

    @Test
    void supportsConfiguration_returns_false_if_kms_keyId_is_null() {
        when(topicConfig.getKmsConfig()).thenReturn(kmsConfig);
        assertThat(createObjectUnderTest().supportsConfiguration(topicConfig), equalTo(false));
    }

    @Test
    void supportsConfiguration_returns_true_if_kms_keyId_is_present() {
        when(topicConfig.getKmsConfig()).thenReturn(kmsConfig);
        when(kmsConfig.getKeyId()).thenReturn(UUID.randomUUID().toString());
        assertThat(createObjectUnderTest().supportsConfiguration(topicConfig), equalTo(true));
    }

    @Nested
    class WithKmsKey {
        private String kmsKeyId;

        private Region region;

        private String encryptionKey;
        private KmsClientBuilder kmsClientBuilder;
        private KmsClient kmsClient;
        private byte[] decryptedBytes;

        @BeforeEach
        void setUp() {
            kmsKeyId = UUID.randomUUID().toString();
            region = mock(Region.class);

            when(awsContext.get()).thenReturn(awsCredentialsProvider);
            when(awsContext.getRegion()).thenReturn(region);

            encryptionKey = UUID.randomUUID().toString();
            String base64EncryptionKey = Base64.getEncoder().encodeToString(encryptionKey.getBytes());
            when(topicConfig.getEncryptionKey()).thenReturn(base64EncryptionKey);
            when(topicConfig.getKmsConfig()).thenReturn(kmsConfig);
            when(kmsConfig.getKeyId()).thenReturn(kmsKeyId);

            kmsClient = mock(KmsClient.class);
            DecryptResponse decryptResponse = mock(DecryptResponse.class);
            decryptedBytes = UUID.randomUUID().toString().getBytes();
            when(decryptResponse.plaintext()).thenReturn(SdkBytes.fromByteArray(decryptedBytes));
            when(kmsClient.decrypt(any(Consumer.class))).thenReturn(decryptResponse);

            kmsClientBuilder = mock(KmsClientBuilder.class);
            when(kmsClientBuilder.credentialsProvider(awsCredentialsProvider)).thenReturn(kmsClientBuilder);
            when(kmsClientBuilder.region(region)).thenReturn(kmsClientBuilder);
            when(kmsClientBuilder.build()).thenReturn(kmsClient);
        }

        @Test
        void apply_returns_plaintext_from_decrypt_request() {
            byte[] actualBytes;
            KmsKeyProvider objectUnderTest = createObjectUnderTest();
            try (MockedStatic<KmsClient> kmsClientMockedStatic = mockStatic(KmsClient.class)) {
                kmsClientMockedStatic.when(() -> KmsClient.builder()).thenReturn(kmsClientBuilder);
                actualBytes = objectUnderTest.apply(topicConfig);
            }

            assertThat(actualBytes, equalTo(decryptedBytes));
        }

        @Test
        void apply_calls_decrypt_with_correct_values_when_encryption_context_is_null() {
            KmsKeyProvider objectUnderTest = createObjectUnderTest();

            when(kmsConfig.getEncryptionContext()).thenReturn(null);

            try (MockedStatic<KmsClient> kmsClientMockedStatic = mockStatic(KmsClient.class)) {
                kmsClientMockedStatic.when(() -> KmsClient.builder()).thenReturn(kmsClientBuilder);
                objectUnderTest.apply(topicConfig);
            }

            ArgumentCaptor<Consumer<DecryptRequest.Builder>> consumerArgumentCaptor = ArgumentCaptor.forClass(Consumer.class);
            verify(kmsClient).decrypt(consumerArgumentCaptor.capture());

            Consumer<DecryptRequest.Builder> actualConsumer = consumerArgumentCaptor.getValue();

            DecryptRequest.Builder builder = mock(DecryptRequest.Builder.class);
            when(builder.keyId(anyString())).thenReturn(builder);
            when(builder.ciphertextBlob(any())).thenReturn(builder);
            when(builder.encryptionContext(any())).thenReturn(builder);
            actualConsumer.accept(builder);

            verify(builder).keyId(kmsKeyId);
            ArgumentCaptor<SdkBytes> actualBytesCaptor = ArgumentCaptor.forClass(SdkBytes.class);
            verify(builder).ciphertextBlob(actualBytesCaptor.capture());

            SdkBytes actualSdkBytes = actualBytesCaptor.getValue();
            assertThat(actualSdkBytes.asByteArray(), equalTo(encryptionKey.getBytes()));

            verify(builder).encryptionContext(isNull());
        }

        @Test
        void apply_calls_decrypt_with_correct_values_when_encryption_context_is_present() {
            Map<String, String> encryptionContext = Map.of(
                    UUID.randomUUID().toString(), UUID.randomUUID().toString(),
                    UUID.randomUUID().toString(), UUID.randomUUID().toString(),
                    UUID.randomUUID().toString(), UUID.randomUUID().toString()
            );
            KmsKeyProvider objectUnderTest = createObjectUnderTest();

            when(kmsConfig.getEncryptionContext()).thenReturn(encryptionContext);

            try (MockedStatic<KmsClient> kmsClientMockedStatic = mockStatic(KmsClient.class)) {
                kmsClientMockedStatic.when(() -> KmsClient.builder()).thenReturn(kmsClientBuilder);
                objectUnderTest.apply(topicConfig);
            }

            ArgumentCaptor<Consumer<DecryptRequest.Builder>> consumerArgumentCaptor = ArgumentCaptor.forClass(Consumer.class);
            verify(kmsClient).decrypt(consumerArgumentCaptor.capture());

            Consumer<DecryptRequest.Builder> actualConsumer = consumerArgumentCaptor.getValue();

            DecryptRequest.Builder builder = mock(DecryptRequest.Builder.class);
            when(builder.keyId(anyString())).thenReturn(builder);
            when(builder.ciphertextBlob(any())).thenReturn(builder);
            when(builder.encryptionContext(any())).thenReturn(builder);
            actualConsumer.accept(builder);

            verify(builder).keyId(kmsKeyId);
            ArgumentCaptor<SdkBytes> actualBytesCaptor = ArgumentCaptor.forClass(SdkBytes.class);
            verify(builder).ciphertextBlob(actualBytesCaptor.capture());

            SdkBytes actualSdkBytes = actualBytesCaptor.getValue();
            assertThat(actualSdkBytes.asByteArray(), equalTo(encryptionKey.getBytes()));

            verify(builder).encryptionContext(encryptionContext);
        }
    }
}