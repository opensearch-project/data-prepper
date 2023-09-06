/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafkaconnect.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClientBuilder;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ScretManagerHelperTest {
    private final String expectedSecretString = "expectedSecret";
    private final String testStsRole = "testRole";
    private final String testRegion = "testRegion";
    private final String testSecretId = "testSecritId";
    @Mock
    private SecretsManagerClientBuilder secretsManagerClientBuilder;
    @Mock
    private SecretsManagerClient secretsManagerClient;
    @Mock
    private GetSecretValueResponse getSecretValueResponse;

    @BeforeEach
    void setup() {
        secretsManagerClientBuilder = mock(SecretsManagerClientBuilder.class);
        secretsManagerClient = mock(SecretsManagerClient.class);
        getSecretValueResponse = mock(GetSecretValueResponse.class);
        lenient().when(secretsManagerClientBuilder.overrideConfiguration(any(ClientOverrideConfiguration.class))).thenReturn(secretsManagerClientBuilder);
        lenient().when(secretsManagerClientBuilder.credentialsProvider(any(AwsCredentialsProvider.class))).thenReturn(secretsManagerClientBuilder);
        lenient().when(secretsManagerClientBuilder.region(any())).thenReturn(secretsManagerClientBuilder);
        lenient().when(secretsManagerClientBuilder.build()).thenReturn(secretsManagerClient);
        lenient().when(secretsManagerClient.getSecretValue(any(GetSecretValueRequest.class))).thenReturn(getSecretValueResponse);
        lenient().when(getSecretValueResponse.secretString()).thenReturn(expectedSecretString);
    }

    @Test
    void test_get_secret_without_sts() {
        try (MockedStatic<SecretsManagerClient> mockedStatic = mockStatic(SecretsManagerClient.class)) {
            mockedStatic.when(() -> SecretsManagerClient.builder()).thenReturn(secretsManagerClientBuilder);
            String result = SecretManagerHelper.getSecretValue("", testRegion, testSecretId);
            assertThat(result, is(expectedSecretString));
            verify(secretsManagerClientBuilder, times(1)).credentialsProvider(any(AwsCredentialsProvider.class));
        }
    }

    @Test
    void test_get_secret_with_sts() {
        try (MockedStatic<StsClient> mockedSts = mockStatic(StsClient.class);
             MockedStatic<SecretsManagerClient> mockedStatic = mockStatic(SecretsManagerClient.class)) {
            StsClient stsClient = mock(StsClient.class);
            StsClientBuilder stsClientBuilder = mock(StsClientBuilder.class);
            when(stsClientBuilder.overrideConfiguration(any(ClientOverrideConfiguration.class))).thenReturn(stsClientBuilder);
            when(stsClientBuilder.credentialsProvider(any(AwsCredentialsProvider.class))).thenReturn(stsClientBuilder);
            when(stsClientBuilder.region(any())).thenReturn(stsClientBuilder);
            when(stsClientBuilder.build()).thenReturn(stsClient);

            mockedSts.when(() -> StsClient.builder()).thenReturn(stsClientBuilder);
            mockedStatic.when(() -> SecretsManagerClient.builder()).thenReturn(secretsManagerClientBuilder);
            String result = SecretManagerHelper.getSecretValue(testStsRole, testRegion, testSecretId);
            assertThat(result, is(expectedSecretString));
            verify(secretsManagerClientBuilder, times(1)).credentialsProvider(any(StsAssumeRoleCredentialsProvider.class));
        }
    }
}
