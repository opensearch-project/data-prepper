/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.http;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.aws.api.AwsConfig;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;

import java.net.URI;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AwsAuthenticationDecoratorTest {

    private AwsCredentialsSupplier awsCredentialsSupplier;
    private AwsCredentialsProvider awsCredentialsProvider;
    private AwsConfig awsConfig;

    private static final String TEST_ROLE_ARN = "arn:aws:iam::123456789012:role/TestRole";
    private static final String TEST_EXTERNAL_ID = "test-external-id";
    private static final Region TEST_REGION = Region.US_EAST_1;
    private static final Map<String, String> TEST_HEADER_OVERRIDES = Map.of("headerKey", "headerValue");

    @BeforeEach
    void setUp() {
        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        awsCredentialsProvider = mock(AwsCredentialsProvider.class);
        awsConfig = mock(AwsConfig.class);

        when(awsConfig.getAwsRegion()).thenReturn(TEST_REGION);
        when(awsConfig.getAwsStsRoleArn()).thenReturn(TEST_ROLE_ARN);
        when(awsConfig.getAwsStsExternalId()).thenReturn(TEST_EXTERNAL_ID);
        when(awsConfig.getAwsStsHeaderOverrides()).thenReturn(TEST_HEADER_OVERRIDES);
        when(awsCredentialsSupplier.getProvider(any(AwsCredentialsOptions.class))).thenReturn(awsCredentialsProvider);
        when(awsCredentialsProvider.resolveCredentials())
                .thenReturn(AwsBasicCredentials.create("testAccessKey", "testSecretKey"));
    }

    private AwsAuthenticationDecorator createObjectUnderTest() {
        return new AwsAuthenticationDecorator(awsCredentialsSupplier, awsConfig);
    }

    private SdkHttpFullRequest createTestRequest() {
        return SdkHttpFullRequest.builder()
                .method(SdkHttpMethod.POST)
                .uri(URI.create("https://example.com/test"))
                .putHeader("Content-Type", "application/json")
                .contentStreamProvider(() -> SdkBytes.fromUtf8String("{\"key\":\"value\"}").asInputStream())
                .build();
    }

    @Test
    void test_constructor_passes_correct_credentials_options() {
        final ArgumentCaptor<AwsCredentialsOptions> optionsCaptor =
                ArgumentCaptor.forClass(AwsCredentialsOptions.class);

        createObjectUnderTest();

        verify(awsCredentialsSupplier).getProvider(optionsCaptor.capture());
        final AwsCredentialsOptions capturedOptions = optionsCaptor.getValue();

        assertThat(capturedOptions.getRegion(), equalTo(TEST_REGION));
        assertThat(capturedOptions.getStsRoleArn(), equalTo(TEST_ROLE_ARN));
        assertThat(capturedOptions.getStsExternalId(), equalTo(TEST_EXTERNAL_ID));
        assertThat(capturedOptions.getStsHeaderOverrides(), equalTo(TEST_HEADER_OVERRIDES));
    }

    @Test
    void test_authenticate_adds_content_sha256_header() {
        final AwsAuthenticationDecorator decorator = createObjectUnderTest();
        final SdkHttpFullRequest request = createTestRequest();

        final SdkHttpFullRequest signedRequest = decorator.authenticate(request);

        assertThat(signedRequest.headers(), hasKey("x-amz-content-sha256"));
        assertThat(signedRequest.headers().get("x-amz-content-sha256").get(0).length(), equalTo(64));
    }

    @Test
    void test_authenticate_adds_authorization_header() {
        final AwsAuthenticationDecorator decorator = createObjectUnderTest();
        final SdkHttpFullRequest request = createTestRequest();

        final SdkHttpFullRequest signedRequest = decorator.authenticate(request);

        assertThat(signedRequest.headers(), hasKey("Authorization"));
        final String authHeader = signedRequest.headers().get("Authorization").get(0);
        assertTrue(authHeader.startsWith("AWS4-HMAC-SHA256"), "Authorization header should use AWS4-HMAC-SHA256 signing");
    }

    @Test
    void test_authenticate_signs_with_correct_region() {
        final AwsAuthenticationDecorator decorator = createObjectUnderTest();
        final SdkHttpFullRequest request = createTestRequest();

        final SdkHttpFullRequest signedRequest = decorator.authenticate(request);

        final String authHeader = signedRequest.headers().get("Authorization").get(0);
        assertTrue(authHeader.contains(TEST_REGION.id()), "Authorization header should contain the configured region");
    }

    @Test
    void test_authenticate_signs_with_correct_service_name() {
        final AwsAuthenticationDecorator decorator = createObjectUnderTest();
        final SdkHttpFullRequest request = createTestRequest();

        final SdkHttpFullRequest signedRequest = decorator.authenticate(request);

        final String authHeader = signedRequest.headers().get("Authorization").get(0);
        assertTrue(authHeader.contains("execute-api"), "Authorization header should contain the service signing name");
    }

    @Test
    void test_authenticate_preserves_original_headers() {
        final AwsAuthenticationDecorator decorator = createObjectUnderTest();
        final SdkHttpFullRequest request = createTestRequest();

        final SdkHttpFullRequest signedRequest = decorator.authenticate(request);

        assertThat(signedRequest.headers(), hasKey("Content-Type"));
        assertThat(signedRequest.headers().get("Content-Type"), equalTo(List.of("application/json")));
    }

    @Test
    void test_authenticate_preserves_request_method_and_uri() {
        final AwsAuthenticationDecorator decorator = createObjectUnderTest();
        final SdkHttpFullRequest request = createTestRequest();

        final SdkHttpFullRequest signedRequest = decorator.authenticate(request);

        assertThat(signedRequest.method(), equalTo(SdkHttpMethod.POST));
        assertThat(signedRequest.getUri(), equalTo(URI.create("https://example.com/test")));
    }

    @Test
    void test_authenticate_returns_non_null_result() {
        final AwsAuthenticationDecorator decorator = createObjectUnderTest();
        final SdkHttpFullRequest request = createTestRequest();

        final SdkHttpFullRequest signedRequest = decorator.authenticate(request);

        assertThat(signedRequest, notNullValue());
    }

    @Test
    void test_authenticate_resolves_credentials_on_each_call() {
        final AwsAuthenticationDecorator decorator = createObjectUnderTest();
        final SdkHttpFullRequest request = createTestRequest();

        decorator.authenticate(request);
        decorator.authenticate(request);

        verify(awsCredentialsProvider, org.mockito.Mockito.times(2)).resolveCredentials();
    }

    @Test
    void test_constructor_with_null_supplier_throws_exception() {
        assertThrows(NullPointerException.class, () -> new AwsAuthenticationDecorator(null, awsConfig));
    }

    @Test
    void test_constructor_with_null_config_throws_exception() {
        assertThrows(NullPointerException.class, () -> new AwsAuthenticationDecorator(awsCredentialsSupplier, null));
    }
}
