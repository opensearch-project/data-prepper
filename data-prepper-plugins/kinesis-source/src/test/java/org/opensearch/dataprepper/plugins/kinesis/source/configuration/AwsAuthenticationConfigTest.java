package org.opensearch.dataprepper.plugins.kinesis.source.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AwsAuthenticationConfigTest {
    private ObjectMapper objectMapper = new ObjectMapper();
    private final String TEST_ROLE = "arn:aws:iam::123456789012:role/test-role";

    @ParameterizedTest
    @ValueSource(strings = {"us-east-1", "us-west-2", "eu-central-1"})
    void getAwsRegionReturnsRegion(final String regionString) {
        final Region expectedRegionObject = Region.of(regionString);
        final Map<String, Object> jsonMap = Map.of("region", regionString);
        final AwsAuthenticationConfig objectUnderTest = objectMapper.convertValue(jsonMap, AwsAuthenticationConfig.class);
        assertThat(objectUnderTest.getAwsRegion(), equalTo(expectedRegionObject));
    }

    @Test
    void getAwsRegionReturnsNullWhenRegionIsNull() {
        final Map<String, Object> jsonMap = Collections.emptyMap();
        final AwsAuthenticationConfig objectUnderTest = objectMapper.convertValue(jsonMap, AwsAuthenticationConfig.class);
        assertThat(objectUnderTest.getAwsRegion(), nullValue());
    }

    @Test
    void getAwsStsRoleArnReturnsValueFromDeserializedJSON() {
        final String stsRoleArn = UUID.randomUUID().toString();
        final Map<String, Object> jsonMap = Map.of("sts_role_arn", stsRoleArn);
        final AwsAuthenticationConfig objectUnderTest = objectMapper.convertValue(jsonMap, AwsAuthenticationConfig.class);
        assertThat(objectUnderTest.getAwsStsRoleArn(), equalTo(stsRoleArn));
    }

    @Test
    void getAwsStsRoleArnReturnsNullIfNotInJSON() {
        final Map<String, Object> jsonMap = Collections.emptyMap();
        final AwsAuthenticationConfig objectUnderTest = objectMapper.convertValue(jsonMap, AwsAuthenticationConfig.class);
        assertThat(objectUnderTest.getAwsStsRoleArn(), nullValue());
    }

    @Test
    void getAwsStsExternalIdReturnsValueFromDeserializedJSON() {
        final String stsExternalId = UUID.randomUUID().toString();
        final Map<String, Object> jsonMap = Map.of("sts_external_id", stsExternalId);
        final AwsAuthenticationConfig objectUnderTest = objectMapper.convertValue(jsonMap, AwsAuthenticationConfig.class);
        assertThat(objectUnderTest.getAwsStsExternalId(), equalTo(stsExternalId));
    }

    @Test
    void getAwsStsExternalIdReturnsNullIfNotInJSON() {
        final Map<String, Object> jsonMap = Collections.emptyMap();
        final AwsAuthenticationConfig objectUnderTest = objectMapper.convertValue(jsonMap, AwsAuthenticationConfig.class);
        assertThat(objectUnderTest.getAwsStsExternalId(), nullValue());
    }

    @Test
    void getAwsStsHeaderOverridesReturnsValueFromDeserializedJSON() {
        final Map<String, String> stsHeaderOverrides = Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        final Map<String, Object> jsonMap = Map.of("sts_header_overrides", stsHeaderOverrides);
        final AwsAuthenticationConfig objectUnderTest = objectMapper.convertValue(jsonMap, AwsAuthenticationConfig.class);
        assertThat(objectUnderTest.getAwsStsHeaderOverrides(), equalTo(stsHeaderOverrides));
    }

    @Test
    void getAwsStsHeaderOverridesReturnsNullIfNotInJSON() {
        final Map<String, Object> jsonMap = Collections.emptyMap();
        final AwsAuthenticationConfig objectUnderTest = objectMapper.convertValue(jsonMap, AwsAuthenticationConfig.class);
        assertThat(objectUnderTest.getAwsStsHeaderOverrides(), nullValue());
    }

    @Test
    void authenticateAWSConfigurationShouldReturnWithoutStsRoleArn() throws NoSuchFieldException, IllegalAccessException {
        AwsAuthenticationConfig awsAuthenticationOptionsConfig = new AwsAuthenticationConfig();
        ReflectivelySetField.setField(AwsAuthenticationConfig.class, awsAuthenticationOptionsConfig, "awsRegion", "us-east-1");
        ReflectivelySetField.setField(AwsAuthenticationConfig.class, awsAuthenticationOptionsConfig, "awsStsRoleArn", null);

        final DefaultCredentialsProvider mockedCredentialsProvider = mock(DefaultCredentialsProvider.class);
        final AwsCredentialsProvider actualCredentialsProvider;
        try (final MockedStatic<DefaultCredentialsProvider> defaultCredentialsProviderMockedStatic = mockStatic(DefaultCredentialsProvider.class)) {
            defaultCredentialsProviderMockedStatic.when(DefaultCredentialsProvider::create)
                    .thenReturn(mockedCredentialsProvider);
            actualCredentialsProvider = awsAuthenticationOptionsConfig.authenticateAwsConfiguration();
        }

        assertThat(actualCredentialsProvider, sameInstance(mockedCredentialsProvider));
    }


    @Nested
    class WithSts {
        private StsClient stsClient;
        private StsClientBuilder stsClientBuilder;

        @BeforeEach
        void setUp() {
            stsClient = mock(StsClient.class);
            stsClientBuilder = mock(StsClientBuilder.class);

            when(stsClientBuilder.build()).thenReturn(stsClient);
        }

        @Test
        void authenticateAWSConfigurationShouldReturnWithStsRoleArn() throws NoSuchFieldException, IllegalAccessException {
            AwsAuthenticationConfig awsAuthenticationOptionsConfig = new AwsAuthenticationConfig();
            ReflectivelySetField.setField(AwsAuthenticationConfig.class, awsAuthenticationOptionsConfig, "awsRegion", "us-east-1");
            ReflectivelySetField.setField(AwsAuthenticationConfig.class, awsAuthenticationOptionsConfig, "awsStsRoleArn", TEST_ROLE);

            when(stsClientBuilder.region(Region.US_EAST_1)).thenReturn(stsClientBuilder);
            final AssumeRoleRequest.Builder assumeRoleRequestBuilder = mock(AssumeRoleRequest.Builder.class);
            when(assumeRoleRequestBuilder.roleSessionName(anyString()))
                    .thenReturn(assumeRoleRequestBuilder);
            when(assumeRoleRequestBuilder.roleArn(anyString()))
                    .thenReturn(assumeRoleRequestBuilder);

            final AwsCredentialsProvider actualCredentialsProvider;
            try (final MockedStatic<StsClient> stsClientMockedStatic = mockStatic(StsClient.class);
                 final MockedStatic<AssumeRoleRequest> assumeRoleRequestMockedStatic = mockStatic(AssumeRoleRequest.class)) {
                stsClientMockedStatic.when(StsClient::builder).thenReturn(stsClientBuilder);
                assumeRoleRequestMockedStatic.when(AssumeRoleRequest::builder).thenReturn(assumeRoleRequestBuilder);
                actualCredentialsProvider = awsAuthenticationOptionsConfig.authenticateAwsConfiguration();
            }

            assertThat(actualCredentialsProvider, instanceOf(AwsCredentialsProvider.class));

            verify(assumeRoleRequestBuilder).roleArn(TEST_ROLE);
            verify(assumeRoleRequestBuilder).roleSessionName(anyString());
            verify(assumeRoleRequestBuilder).build();
            verifyNoMoreInteractions(assumeRoleRequestBuilder);
        }

        @Test
        void authenticateAWSConfigurationShouldReturnWithStsRoleArnWhenNoRegion() throws NoSuchFieldException, IllegalAccessException {
            AwsAuthenticationConfig awsAuthenticationOptionsConfig = new AwsAuthenticationConfig();
            ReflectivelySetField.setField(AwsAuthenticationConfig.class, awsAuthenticationOptionsConfig, "awsRegion", null);
            ReflectivelySetField.setField(AwsAuthenticationConfig.class, awsAuthenticationOptionsConfig, "awsStsRoleArn", TEST_ROLE);
            assertThat(awsAuthenticationOptionsConfig.getAwsRegion(), CoreMatchers.equalTo(null));

            when(stsClientBuilder.region(null)).thenReturn(stsClientBuilder);

            final AwsCredentialsProvider actualCredentialsProvider;
            try (final MockedStatic<StsClient> stsClientMockedStatic = mockStatic(StsClient.class)) {
                stsClientMockedStatic.when(StsClient::builder).thenReturn(stsClientBuilder);
                actualCredentialsProvider = awsAuthenticationOptionsConfig.authenticateAwsConfiguration();
            }

            assertThat(actualCredentialsProvider, instanceOf(AwsCredentialsProvider.class));
        }

        @Test
        void authenticateAWSConfigurationShouldOverrideSTSHeadersWhenHeaderOverridesSet() throws NoSuchFieldException, IllegalAccessException {
            final String headerName1 = UUID.randomUUID().toString();
            final String headerValue1 = UUID.randomUUID().toString();
            final String headerName2 = UUID.randomUUID().toString();
            final String headerValue2 = UUID.randomUUID().toString();
            final Map<String, String> overrideHeaders = Map.of(headerName1, headerValue1, headerName2, headerValue2);

            AwsAuthenticationConfig awsAuthenticationOptionsConfig = new AwsAuthenticationConfig();
            ReflectivelySetField.setField(AwsAuthenticationConfig.class, awsAuthenticationOptionsConfig, "awsRegion", "us-east-1");
            ReflectivelySetField.setField(AwsAuthenticationConfig.class, awsAuthenticationOptionsConfig, "awsStsRoleArn", TEST_ROLE);
            ReflectivelySetField.setField(AwsAuthenticationConfig.class, awsAuthenticationOptionsConfig, "awsStsHeaderOverrides", overrideHeaders);

            when(stsClientBuilder.region(Region.US_EAST_1)).thenReturn(stsClientBuilder);

            final AssumeRoleRequest.Builder assumeRoleRequestBuilder = mock(AssumeRoleRequest.Builder.class);
            when(assumeRoleRequestBuilder.roleSessionName(anyString()))
                    .thenReturn(assumeRoleRequestBuilder);
            when(assumeRoleRequestBuilder.roleArn(anyString()))
                    .thenReturn(assumeRoleRequestBuilder);
            when(assumeRoleRequestBuilder.overrideConfiguration(any(Consumer.class)))
                    .thenReturn(assumeRoleRequestBuilder);

            final AwsCredentialsProvider actualCredentialsProvider;
            try (final MockedStatic<StsClient> stsClientMockedStatic = mockStatic(StsClient.class);
                 final MockedStatic<AssumeRoleRequest> assumeRoleRequestMockedStatic = mockStatic(AssumeRoleRequest.class)) {
                stsClientMockedStatic.when(StsClient::builder).thenReturn(stsClientBuilder);
                assumeRoleRequestMockedStatic.when(AssumeRoleRequest::builder).thenReturn(assumeRoleRequestBuilder);
                actualCredentialsProvider = awsAuthenticationOptionsConfig.authenticateAwsConfiguration();
            }

            assertThat(actualCredentialsProvider, instanceOf(AwsCredentialsProvider.class));

            final ArgumentCaptor<Consumer<AwsRequestOverrideConfiguration.Builder>> configurationCaptor = ArgumentCaptor.forClass(Consumer.class);

            verify(assumeRoleRequestBuilder).roleArn(TEST_ROLE);
            verify(assumeRoleRequestBuilder).roleSessionName(anyString());
            verify(assumeRoleRequestBuilder).overrideConfiguration(configurationCaptor.capture());
            verify(assumeRoleRequestBuilder).build();
            verifyNoMoreInteractions(assumeRoleRequestBuilder);

            final Consumer<AwsRequestOverrideConfiguration.Builder> actualOverride = configurationCaptor.getValue();

            final AwsRequestOverrideConfiguration.Builder configurationBuilder = mock(AwsRequestOverrideConfiguration.Builder.class);
            actualOverride.accept(configurationBuilder);
            verify(configurationBuilder).putHeader(headerName1, headerValue1);
            verify(configurationBuilder).putHeader(headerName2, headerValue2);
            verifyNoMoreInteractions(configurationBuilder);
        }

        @Test
        void authenticateAWSConfigurationShouldNotOverrideSTSHeadersWhenHeaderOverridesAreEmpty() throws NoSuchFieldException, IllegalAccessException {

            AwsAuthenticationConfig awsAuthenticationOptionsConfig = new AwsAuthenticationConfig();
            ReflectivelySetField.setField(AwsAuthenticationConfig.class, awsAuthenticationOptionsConfig, "awsRegion", "us-east-1");
            ReflectivelySetField.setField(AwsAuthenticationConfig.class, awsAuthenticationOptionsConfig, "awsStsRoleArn", TEST_ROLE);
            ReflectivelySetField.setField(AwsAuthenticationConfig.class, awsAuthenticationOptionsConfig, "awsStsHeaderOverrides", Collections.emptyMap());

            when(stsClientBuilder.region(Region.US_EAST_1)).thenReturn(stsClientBuilder);
            final AssumeRoleRequest.Builder assumeRoleRequestBuilder = mock(AssumeRoleRequest.Builder.class);
            when(assumeRoleRequestBuilder.roleSessionName(anyString()))
                    .thenReturn(assumeRoleRequestBuilder);
            when(assumeRoleRequestBuilder.roleArn(anyString()))
                    .thenReturn(assumeRoleRequestBuilder);

            final AwsCredentialsProvider actualCredentialsProvider;
            try (final MockedStatic<StsClient> stsClientMockedStatic = mockStatic(StsClient.class);
                 final MockedStatic<AssumeRoleRequest> assumeRoleRequestMockedStatic = mockStatic(AssumeRoleRequest.class)) {
                stsClientMockedStatic.when(StsClient::builder).thenReturn(stsClientBuilder);
                assumeRoleRequestMockedStatic.when(AssumeRoleRequest::builder).thenReturn(assumeRoleRequestBuilder);
                actualCredentialsProvider = awsAuthenticationOptionsConfig.authenticateAwsConfiguration();
            }

            assertThat(actualCredentialsProvider, instanceOf(AwsCredentialsProvider.class));

            verify(assumeRoleRequestBuilder).roleArn(TEST_ROLE);
            verify(assumeRoleRequestBuilder).roleSessionName(anyString());
            verify(assumeRoleRequestBuilder).build();
            verifyNoMoreInteractions(assumeRoleRequestBuilder);
        }
    }
}
