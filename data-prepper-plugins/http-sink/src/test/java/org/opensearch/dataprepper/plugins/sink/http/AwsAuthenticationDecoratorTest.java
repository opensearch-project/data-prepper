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

import com.linecorp.armeria.common.HttpRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.aws.api.AwsConfig;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
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

    private static final String TEST_URL = "https://example.com/test";
    private static final byte[] TEST_PAYLOAD = "{\"key\":\"value\"}".getBytes(StandardCharsets.UTF_8);
    private static final String TEST_ROLE_ARN = "arn:aws:iam::123456789012:role/TestRole";
    private static final String TEST_EXTERNAL_ID = "test-external-id";
    private static final Region TEST_REGION = Region.US_EAST_1;
    private static final Map<String, String> TEST_HEADER_OVERRIDES = Map.of("headerKey", "headerValue");
    private static final String TEST_SERVICE_NAME = "execute-api";

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
        return new AwsAuthenticationDecorator(awsCredentialsSupplier, awsConfig, TEST_SERVICE_NAME);
    }

    private AwsAuthenticationDecorator createObjectUnderTest(final String serviceName) {
        return new AwsAuthenticationDecorator(awsCredentialsSupplier, awsConfig, serviceName);
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
    void test_buildRequest_returns_non_null_http_request() {
        final AwsAuthenticationDecorator decorator = createObjectUnderTest();

        final HttpRequest request = decorator.buildRequest(TEST_URL, TEST_PAYLOAD, null);

        assertThat(request, notNullValue());
    }

    @Test
    void test_buildRequest_contains_authorization_header() {
        final AwsAuthenticationDecorator decorator = createObjectUnderTest();

        final HttpRequest request = decorator.buildRequest(TEST_URL, TEST_PAYLOAD, null);

        assertTrue(request.headers().contains("Authorization"), "Request should contain Authorization header");
        final String authHeader = request.headers().get("Authorization");
        assertTrue(authHeader.startsWith("AWS4-HMAC-SHA256"), "Authorization header should use AWS4-HMAC-SHA256 signing");
    }

    @Test
    void test_buildRequest_signs_with_correct_region() {
        final AwsAuthenticationDecorator decorator = createObjectUnderTest();

        final HttpRequest request = decorator.buildRequest(TEST_URL, TEST_PAYLOAD, null);

        final String authHeader = request.headers().get("Authorization");
        assertTrue(authHeader.contains(TEST_REGION.id()), "Authorization header should contain the configured region");
    }

    @Test
    void test_buildRequest_signs_with_configured_service_name() {
        final AwsAuthenticationDecorator decorator = createObjectUnderTest();

        final HttpRequest request = decorator.buildRequest(TEST_URL, TEST_PAYLOAD, null);

        final String authHeader = request.headers().get("Authorization");
        assertTrue(authHeader.contains(TEST_SERVICE_NAME), "Authorization header should contain the service signing name");
    }

    @Test
    void test_buildRequest_signs_with_custom_service_name() {
        final String customServiceName = "osis";
        final AwsAuthenticationDecorator decorator = createObjectUnderTest(customServiceName);

        final HttpRequest request = decorator.buildRequest(TEST_URL, TEST_PAYLOAD, null);

        final String authHeader = request.headers().get("Authorization");
        assertTrue(authHeader.contains(customServiceName), "Authorization header should contain the custom service signing name");
    }

    @Test
    void test_buildRequest_contains_content_sha256_header() {
        final AwsAuthenticationDecorator decorator = createObjectUnderTest();

        final HttpRequest request = decorator.buildRequest(TEST_URL, TEST_PAYLOAD, null);

        assertTrue(request.headers().contains("x-amz-content-sha256"), "Request should contain x-amz-content-sha256 header");
        assertThat(request.headers().get("x-amz-content-sha256").length(), equalTo(64));
    }

    @Test
    void test_buildRequest_includes_custom_headers() {
        final AwsAuthenticationDecorator decorator = createObjectUnderTest();
        final Map<String, List<String>> customHeaders = Map.of(
                "X-Custom-Header", List.of("custom-value"),
                "X-Another", List.of("val1", "val2")
        );

        final HttpRequest request = decorator.buildRequest(TEST_URL, TEST_PAYLOAD, customHeaders);

        assertTrue(request.headers().contains("X-Custom-Header"), "Request should contain custom header");
        assertThat(request.headers().get("X-Custom-Header"), equalTo("custom-value"));
    }

    @Test
    void test_buildRequest_preserves_url_path() {
        final AwsAuthenticationDecorator decorator = createObjectUnderTest();

        final HttpRequest request = decorator.buildRequest(TEST_URL, TEST_PAYLOAD, null);

        assertThat(request.path(), equalTo("/test"));
    }

    @Test
    void test_buildRequest_preserves_url_authority() {
        final AwsAuthenticationDecorator decorator = createObjectUnderTest();

        final HttpRequest request = decorator.buildRequest(TEST_URL, TEST_PAYLOAD, null);

        assertThat(request.authority(), equalTo("example.com"));
    }

    @Test
    void test_buildRequest_resolves_credentials_on_each_call() {
        final AwsAuthenticationDecorator decorator = createObjectUnderTest();

        decorator.buildRequest(TEST_URL, TEST_PAYLOAD, null);
        decorator.buildRequest(TEST_URL, TEST_PAYLOAD, null);

        verify(awsCredentialsProvider, org.mockito.Mockito.times(2)).resolveCredentials();
    }

    @Test
    void test_buildRequest_with_null_custom_headers() {
        final AwsAuthenticationDecorator decorator = createObjectUnderTest();

        final HttpRequest request = decorator.buildRequest(TEST_URL, TEST_PAYLOAD, null);

        assertThat(request, notNullValue());
    }

    @Test
    void test_constructor_with_null_supplier_throws_exception() {
        assertThrows(NullPointerException.class, () -> new AwsAuthenticationDecorator(null, awsConfig, TEST_SERVICE_NAME));
    }

    @Test
    void test_constructor_with_null_config_throws_exception() {
        assertThrows(NullPointerException.class, () -> new AwsAuthenticationDecorator(awsCredentialsSupplier, null, TEST_SERVICE_NAME));
    }
}
