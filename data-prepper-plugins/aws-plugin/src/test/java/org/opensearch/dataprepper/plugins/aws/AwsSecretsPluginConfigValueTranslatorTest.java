package org.opensearch.dataprepper.plugins.aws;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.SecretsSupplier;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AwsSecretsPluginConfigValueTranslatorTest {
    @Mock
    private SecretsSupplier secretsSupplier;

    private AwsSecretsPluginConfigValueTranslator objectUnderTest;

    @BeforeEach
    void setUp() {
        objectUnderTest = new AwsSecretsPluginConfigValueTranslator(secretsSupplier);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            "my_secret",
            "invalid secret id with space.secret_key"
    })
    void testTranslateInputNoMatch(final String input) {
        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.translate(input));
    }

    @Test
    void testTranslateInputMatch() {
        final String testSecretName = "valid@secret-manager_name";
        final String testSecretKey = UUID.randomUUID().toString();
        final String testSecretValue = UUID.randomUUID().toString();
        final String input = String.format("%s.%s", testSecretName, testSecretKey);
        when(secretsSupplier.retrieveValue(eq(testSecretName), eq(testSecretKey))).thenReturn(testSecretValue);
        assertThat(objectUnderTest.translate(input), equalTo(testSecretValue));
    }
}