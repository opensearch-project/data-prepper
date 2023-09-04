package org.opensearch.dataprepper.plugins.kafkaconnect.configuration;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.opensearch.dataprepper.plugins.kafkaconnect.util.SecretManagerHelper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mockStatic;

public class CredentialConfigTest {
    private final String testUserName = "testUser";
    private final String testPassword = "testPassword";
    private final String testStsRole = "testRole";
    private final String testRegion = "testRegion";
    private final String testSecretId = "testSecritId";

    @Test
    void test_crendential_config_plaintext() {
        CredentialsConfig credentialsConfig = new CredentialsConfig(
                CredentialsConfig.CredentialType.PLAINTEXT,
                testUserName,
                testPassword,
                testStsRole,
                testRegion,
                testSecretId
        );
        assertThat(credentialsConfig.getUsername(), is(testUserName));
        assertThat(credentialsConfig.getPassword(), is(testPassword));
    }

    @Test
    void test_crendential_config_plaintext_invalid() {
        assertThrows(IllegalArgumentException.class, () -> new CredentialsConfig(
                CredentialsConfig.CredentialType.PLAINTEXT,
                null,
                null,
                testStsRole,
                testRegion,
                testSecretId
        ));
        assertThrows(IllegalArgumentException.class, () -> new CredentialsConfig(
                CredentialsConfig.CredentialType.PLAINTEXT,
                testUserName,
                null,
                testStsRole,
                testRegion,
                testSecretId
        ));
        assertThrows(IllegalArgumentException.class, () -> new CredentialsConfig(
                CredentialsConfig.CredentialType.PLAINTEXT,
                null,
                testPassword,
                testStsRole,
                testRegion,
                testSecretId
        ));
    }

    @Test
    void test_crendential_config_aws() {
        final String expectedSecret = "{\"username\":\"expectedUsername\",\"password\":\"expectedPassword\"}";
        try (MockedStatic<SecretManagerHelper> mockedStatic = mockStatic(SecretManagerHelper.class)) {
            mockedStatic.when(() -> SecretManagerHelper.getSecretValue(testStsRole, testRegion, testSecretId)).thenReturn(expectedSecret);
            CredentialsConfig credentialsConfig = new CredentialsConfig(
                    CredentialsConfig.CredentialType.SECRET_MANAGER,
                    testUserName,
                    testPassword,
                    testStsRole,
                    testRegion,
                    testSecretId
            );
            assertThat(credentialsConfig.getUsername(), is("expectedUsername"));
            assertThat(credentialsConfig.getPassword(), is("expectedPassword"));
        }
    }

    @Test
    void test_crendential_config_aws_failure_on_secretmanager() {
        try (MockedStatic<SecretManagerHelper> mockedStatic = mockStatic(SecretManagerHelper.class)) {
            mockedStatic.when(() -> SecretManagerHelper.getSecretValue(testStsRole, testRegion, testSecretId)).thenThrow(new RuntimeException());
            assertThrows(RuntimeException.class, () -> new CredentialsConfig(
                    CredentialsConfig.CredentialType.SECRET_MANAGER,
                    testUserName,
                    testPassword,
                    testStsRole,
                    testRegion,
                    testSecretId
            ));
            final String invalidSecret = "{}";
            mockedStatic.when(() -> SecretManagerHelper.getSecretValue(testStsRole, testRegion, testSecretId)).thenReturn(invalidSecret);
            assertThrows(RuntimeException.class, () -> new CredentialsConfig(
                    CredentialsConfig.CredentialType.SECRET_MANAGER,
                    testUserName,
                    testPassword,
                    testStsRole,
                    testRegion,
                    testSecretId
            ));
        }
    }

    @Test
    void test_crendential_config_aws_invalid_input() {
        assertThrows(IllegalArgumentException.class, () -> new CredentialsConfig(
                CredentialsConfig.CredentialType.SECRET_MANAGER,
                testUserName,
                testPassword,
                testStsRole,
                null,
                null
        ));
        assertThrows(IllegalArgumentException.class, () -> new CredentialsConfig(
                CredentialsConfig.CredentialType.SECRET_MANAGER,
                testUserName,
                testPassword,
                testStsRole,
                null,
                testSecretId
        ));
        assertThrows(IllegalArgumentException.class, () -> new CredentialsConfig(
                CredentialsConfig.CredentialType.SECRET_MANAGER,
                testUserName,
                testPassword,
                testStsRole,
                testRegion,
                null
        ));
    }
}
