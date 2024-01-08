package org.opensearch.dataprepper.plugins.aws;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SecretValueDecoderTest {
    @Mock
    private GetSecretValueResponse getSecretValueResponse;

    private final SecretValueDecoder objectUnderTest = new SecretValueDecoder();

    @Test
    void testDecodeSecretString() {
        final String testSecret = UUID.randomUUID().toString();
        when(getSecretValueResponse.secretString()).thenReturn(testSecret);
        assertThat(objectUnderTest.decode(getSecretValueResponse), equalTo(testSecret));
    }

    @Test
    void testDecodeSecretBinary() {
        final String testSecret = UUID.randomUUID().toString();
        when(getSecretValueResponse.secretBinary()).thenReturn(SdkBytes.fromUtf8String(testSecret));
        assertThat(objectUnderTest.decode(getSecretValueResponse), equalTo(testSecret));
    }
}