package org.opensearch.dataprepper.plugins.kafka.common.key;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;

import java.util.Base64;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class UnencryptedKeyProviderTest {
    @Mock
    private TopicConfig topicConfig;

    private UnencryptedKeyProvider createObjectUnderTest() {
        return new UnencryptedKeyProvider();
    }

    @Test
    void supportsConfiguration_returns_true() {
        assertThat(createObjectUnderTest().supportsConfiguration(topicConfig), equalTo(true));
    }

    @Test
    void apply_returns_base64_decoded_encryptionKey() {
        String unencodedInput = UUID.randomUUID().toString();
        String base64InputString = Base64.getEncoder().encodeToString(unencodedInput.getBytes());

        when(topicConfig.getEncryptionKey()).thenReturn(base64InputString);

        byte[] actualBytes = createObjectUnderTest().apply(topicConfig);
        assertThat(actualBytes, notNullValue());
        assertThat(actualBytes, equalTo(unencodedInput.getBytes()));
    }

    @Test
    void isKeyEncrypted_returns_true() {
        assertThat(createObjectUnderTest().isKeyEncrypted(), equalTo(false));
    }
}