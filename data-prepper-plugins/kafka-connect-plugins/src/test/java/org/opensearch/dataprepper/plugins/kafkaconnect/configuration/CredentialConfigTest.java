/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

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
    void test_credential_config_plaintext() {
        CredentialsConfig credentialsConfig = new CredentialsConfig(
                new CredentialsConfig.PlainText(testUserName, testPassword), null);
        assertThat(credentialsConfig.getUsername(), is(testUserName));
        assertThat(credentialsConfig.getPassword(), is(testPassword));
    }

    @Test
    void test_credential_config_plaintext_invalid() {
        assertThrows(IllegalArgumentException.class, () -> new CredentialsConfig(
                new CredentialsConfig.PlainText(null, null), null));
        assertThrows(IllegalArgumentException.class, () -> new CredentialsConfig(
                new CredentialsConfig.PlainText(testUserName, null), null));
        assertThrows(IllegalArgumentException.class, () -> new CredentialsConfig(
                new CredentialsConfig.PlainText(null, testPassword), null));
    }

    @Test
    void test_credential_config_secret_manager() {
        final String expectedSecret = "{\"username\":\"expectedUsername\",\"password\":\"expectedPassword\"}";
        try (MockedStatic<SecretManagerHelper> mockedStatic = mockStatic(SecretManagerHelper.class)) {
            mockedStatic.when(() -> SecretManagerHelper.getSecretValue(testStsRole, testRegion, testSecretId)).thenReturn(expectedSecret);
            CredentialsConfig credentialsConfig = new CredentialsConfig(
                    null, new CredentialsConfig.SecretManager(testStsRole, testRegion, testSecretId));
            assertThat(credentialsConfig.getUsername(), is("expectedUsername"));
            assertThat(credentialsConfig.getPassword(), is("expectedPassword"));
        }
    }

    @Test
    void test_credential_config_failure_on_secret_manager() {
        try (MockedStatic<SecretManagerHelper> mockedStatic = mockStatic(SecretManagerHelper.class)) {
            mockedStatic.when(() -> SecretManagerHelper.getSecretValue(testStsRole, testRegion, testSecretId)).thenThrow(new RuntimeException());
            assertThrows(RuntimeException.class, () -> new CredentialsConfig(
                    null, new CredentialsConfig.SecretManager(testStsRole, testRegion, testSecretId)));
            final String invalidSecret = "{}";
            mockedStatic.when(() -> SecretManagerHelper.getSecretValue(testStsRole, testRegion, testSecretId)).thenReturn(invalidSecret);
            assertThrows(RuntimeException.class, () -> new CredentialsConfig(
                    null, new CredentialsConfig.SecretManager(testStsRole, testRegion, testSecretId)));
        }
    }

    @Test
    void test_credential_config_secret_manager_invalid_input() {
        assertThrows(IllegalArgumentException.class, () -> new CredentialsConfig(
                null, new CredentialsConfig.SecretManager(null, null, null)));
        assertThrows(IllegalArgumentException.class, () -> new CredentialsConfig(
                null, new CredentialsConfig.SecretManager(null, null, testSecretId)));
        assertThrows(IllegalArgumentException.class, () -> new CredentialsConfig(
                null, new CredentialsConfig.SecretManager(null, testRegion, null)));
    }

    @Test
    void test_invalid_credential_config() {
        // Must be set
        assertThrows(IllegalArgumentException.class, () -> new CredentialsConfig(null, null));
        // Cannot both set
        assertThrows(IllegalArgumentException.class, () -> new CredentialsConfig(
                new CredentialsConfig.PlainText(testUserName, testPassword),
                new CredentialsConfig.SecretManager(testStsRole, testRegion, testSecretId)
        ));
    }
}
