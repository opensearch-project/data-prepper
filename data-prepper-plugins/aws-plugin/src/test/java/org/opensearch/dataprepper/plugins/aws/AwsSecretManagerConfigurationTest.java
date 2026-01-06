/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClientBuilder;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.PutSecretValueRequest;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AwsSecretManagerConfigurationTest {

    private static final Validator VALIDATOR = Validation.byDefaultProvider()
            .configure()
            .messageInterpolator(new ParameterMessageInterpolator())
            .buildValidatorFactory().getValidator();

    private final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

    @Mock
    private GetSecretValueRequest.Builder getSecretValueRequestBuilder;

    @Mock
    private PutSecretValueRequest.Builder putSecretValueRequestBuilder;

    @Mock
    private GetSecretValueRequest getSecretValueRequest;

    @Mock
    private PutSecretValueRequest putSecretValueRequest;

    @Mock
    private SecretsManagerClientBuilder secretsManagerClientBuilder;

    @Mock
    private SecretsManagerClient secretsManagerClient;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Captor
    private ArgumentCaptor<AwsCredentialsProvider> awsCredentialsProviderArgumentCaptor;

    @BeforeEach
    void setup() {
        objectMapper.registerModule(new JavaTimeModule());
    }

    @Test
    void testAwsSecretManagerConfigurationDefault() throws IOException {
        final InputStream inputStream = AwsSecretPluginConfigTest.class.getResourceAsStream(
                "/test-aws-secret-manager-configuration-default.yaml");
        final AwsSecretManagerConfiguration awsSecretManagerConfiguration = objectMapper.readValue(
                inputStream, AwsSecretManagerConfiguration.class);
        assertThat(awsSecretManagerConfiguration.getAwsSecretId(), equalTo("test-secret"));
        assertThat(awsSecretManagerConfiguration.getAwsRegion(), equalTo(Region.US_EAST_1));
        assertThat(awsSecretManagerConfiguration.getRefreshInterval(), equalTo(Duration.ofHours(1)));
        assertThat(awsSecretManagerConfiguration.isDisableRefresh(), is(false));
    }

    @Test
    void testAwsSecretManagerConfigurationInvalidRefreshInterval() throws IOException {
        final InputStream inputStream = AwsSecretPluginConfigTest.class.getResourceAsStream(
                "/test-aws-secret-manager-configuration-invalid-refresh-interval.yaml");
        final AwsSecretManagerConfiguration awsSecretManagerConfiguration = objectMapper.readValue(
                inputStream, AwsSecretManagerConfiguration.class);
        final Set<ConstraintViolation<AwsSecretManagerConfiguration>> violations = VALIDATOR.validate(
                awsSecretManagerConfiguration);
        assertThat(violations.size(), equalTo(1));
        final ConstraintViolation<AwsSecretManagerConfiguration> violation = violations.stream().findFirst().get();
        assertThat(violation.getMessage(), equalTo("Refresh interval must be at least 1 hour."));
    }

    @Test
    void testAwsSecretManagerConfigurationNullRefreshInterval() throws IOException {
        final InputStream inputStream = AwsSecretPluginConfigTest.class.getResourceAsStream(
                "/test-aws-secret-manager-configuration-null-refresh-interval.yaml");
        final AwsSecretManagerConfiguration awsSecretManagerConfiguration = objectMapper.readValue(
                inputStream, AwsSecretManagerConfiguration.class);
        final Set<ConstraintViolation<AwsSecretManagerConfiguration>> violations = VALIDATOR.validate(
                awsSecretManagerConfiguration);
        assertThat(violations.size(), equalTo(1));
        final ConstraintViolation<AwsSecretManagerConfiguration> violation = violations.stream().findFirst().get();
        assertThat(violation.getMessage(), equalTo("refresh_interval must not be null"));
    }

    @Test
    void testCreateGetSecretValueRequest() throws IOException {
        when(getSecretValueRequestBuilder.secretId(anyString())).thenReturn(getSecretValueRequestBuilder);
        when(getSecretValueRequestBuilder.build()).thenReturn(getSecretValueRequest);
        final InputStream inputStream = AwsSecretPluginConfigTest.class.getResourceAsStream(
                "/test-aws-secret-manager-configuration-default.yaml");
        final AwsSecretManagerConfiguration awsSecretManagerConfiguration = objectMapper.readValue(
                inputStream, AwsSecretManagerConfiguration.class);
        try (final MockedStatic<GetSecretValueRequest> getSecretValueRequestMockedStatic =
                     mockStatic(GetSecretValueRequest.class)) {
            getSecretValueRequestMockedStatic.when(GetSecretValueRequest::builder).thenReturn(
                    getSecretValueRequestBuilder);
            assertThat(awsSecretManagerConfiguration.createGetSecretValueRequest(), is(getSecretValueRequest));
        }
        verify(getSecretValueRequestBuilder).secretId("test-secret");
    }


    @ParameterizedTest
    @ValueSource(strings = {"", "    ", "secretValue", "{\"keyToUpdate\", \"newValue\"}"})
    void testPutSecretValueRequest_construct_put_request(String secretValueToStore) throws IOException {
        when(putSecretValueRequestBuilder.secretId(anyString())).thenReturn(putSecretValueRequestBuilder);
        when(putSecretValueRequestBuilder.secretString(anyString())).thenReturn(putSecretValueRequestBuilder);
        when(putSecretValueRequestBuilder.build()).thenReturn(putSecretValueRequest);
        final InputStream inputStream = AwsSecretPluginConfigTest.class.getResourceAsStream(
                "/test-aws-secret-manager-configuration-default.yaml");
        final AwsSecretManagerConfiguration awsSecretManagerConfiguration = objectMapper.readValue(
                inputStream, AwsSecretManagerConfiguration.class);
        try (final MockedStatic<PutSecretValueRequest> putSecretValueRequestMockedStatic =
                     mockStatic(PutSecretValueRequest.class)) {
            putSecretValueRequestMockedStatic.when(PutSecretValueRequest::builder).thenReturn(
                    putSecretValueRequestBuilder);
            assertThat(awsSecretManagerConfiguration.putSecretValueRequest(secretValueToStore),
                    is(putSecretValueRequest));
        }
        verify(putSecretValueRequestBuilder).secretId("test-secret");
    }

    @Test
    void testCreateSecretManagerClientWithStsCredential() throws IOException {
        final InputStream inputStream = AwsSecretPluginConfigTest.class.getResourceAsStream(
                "/test-aws-secret-manager-configuration-with-sts.yaml");
        final AwsSecretManagerConfiguration awsSecretManagerConfiguration = objectMapper.readValue(
                inputStream, AwsSecretManagerConfiguration.class);
        assertThat(awsSecretManagerConfiguration.getAwsSecretId(), equalTo("test-secret"));
        when(secretsManagerClientBuilder.region(any(Region.class))).thenReturn(secretsManagerClientBuilder);

        final StsAssumeRoleCredentialsProvider stsAssumeRoleCredentialsProvider = mock(StsAssumeRoleCredentialsProvider.class);
        final ArgumentCaptor<AwsCredentialsOptions> awsCredentialsOptionsArgumentCaptor = ArgumentCaptor.forClass(AwsCredentialsOptions.class);
        when(awsCredentialsSupplier.getProvider(awsCredentialsOptionsArgumentCaptor.capture()))
                .thenReturn(stsAssumeRoleCredentialsProvider);

        when(secretsManagerClientBuilder.credentialsProvider(stsAssumeRoleCredentialsProvider))
                .thenReturn(secretsManagerClientBuilder);
        when(secretsManagerClientBuilder.build()).thenReturn(secretsManagerClient);
        try (final MockedStatic<SecretsManagerClient> secretsManagerClientMockedStatic = mockStatic(
                SecretsManagerClient.class)) {
            secretsManagerClientMockedStatic.when(SecretsManagerClient::builder).thenReturn(secretsManagerClientBuilder);
            assertThat(awsSecretManagerConfiguration.createSecretManagerClient(awsCredentialsSupplier), is(secretsManagerClient));
        }
        verify(secretsManagerClientBuilder).credentialsProvider(awsCredentialsProviderArgumentCaptor.capture());
        final AwsCredentialsProvider awsCredentialsProvider = awsCredentialsProviderArgumentCaptor.getValue();
        assertThat(awsCredentialsProvider, instanceOf(StsAssumeRoleCredentialsProvider.class));

        final AwsCredentialsOptions awsCredentialsOptions = awsCredentialsOptionsArgumentCaptor.getValue();
        assertThat(awsCredentialsOptions, notNullValue());
        assertThat(awsCredentialsOptions.getRegion(), equalTo(Region.US_EAST_1));
        assertThat(awsCredentialsOptions.getStsHeaderOverrides(), notNullValue());
        assertThat(awsCredentialsOptions.getStsHeaderOverrides().get("test_key"), equalTo("test_value"));
        assertThat(awsCredentialsOptions.getStsRoleArn(), equalTo("arn:aws:iam::123456789012:role/test-role"));
    }

    @Test
    void testDeserializationMissingName() throws IOException {
        final InputStream inputStream = AwsSecretPluginConfigTest.class.getResourceAsStream(
                "/test-aws-secret-manager-configuration-missing-secret-id.yaml");
        final AwsSecretManagerConfiguration awsSecretManagerConfiguration = objectMapper.readValue(
                inputStream, AwsSecretManagerConfiguration.class);
        final Set<ConstraintViolation<AwsSecretManagerConfiguration>> violations = VALIDATOR.validate(
                awsSecretManagerConfiguration);
        assertThat(violations.size(), equalTo(1));
    }
}