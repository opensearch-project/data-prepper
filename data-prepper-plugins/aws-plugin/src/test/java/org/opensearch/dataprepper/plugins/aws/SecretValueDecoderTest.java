package org.opensearch.dataprepper.plugins.aws;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SecretValueDecoderTest {
    private static final String TEST_SECRET = "hello world";
    @Mock
    private GetSecretValueResponse getSecretValueResponse;

    private final SecretValueDecoder objectUnderTest = new SecretValueDecoder();

    @Test
    void testDecodeSecretString() {
        when(getSecretValueResponse.secretString()).thenReturn(TEST_SECRET);
        assertThat(objectUnderTest.decode(getSecretValueResponse), equalTo(TEST_SECRET));
    }

    @Test
    void testDecodeSecretBinary() {
        when(getSecretValueResponse.secretBinary()).thenReturn(SdkBytes.fromUtf8String(TEST_SECRET));
        assertThat(objectUnderTest.decode(getSecretValueResponse), equalTo(TEST_SECRET));
    }
}