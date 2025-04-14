/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.otlp.http;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.MockedStatic;
import org.opensearch.dataprepper.plugins.sink.otlp.configuration.OtlpSinkConfig;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SigV4SignerTest {

    private static final byte[] PAYLOAD = "test-payload".getBytes(StandardCharsets.UTF_8);
    private OtlpSinkConfig mockXrayConfig;
    private SigV4Signer target;

    @BeforeEach
    void setUp(){
        System.setProperty("aws.accessKeyId", "dummy");
        System.setProperty("aws.secretAccessKey", "dummy");

        mockXrayConfig = mock(OtlpSinkConfig.class);
    }

    @AfterEach
    void cleanUp() {
        System.clearProperty("aws.accessKeyId");
        System.clearProperty("aws.secretAccessKey");
        System.clearProperty("aws.region");
    }

    @Test
    void testSignRequest_withInputEndpoint_whenEndpointIsSet() {
        // setup
        final String endpoint = "https://performance.us-west-2.xray.cloudwatch.aws.dev/v1/traces";
        System.setProperty("aws.region", Region.US_WEST_2.toString());
        when(mockXrayConfig.getAwsRegion()).thenReturn(null);
        when(mockXrayConfig.getEndpoint()).thenReturn(endpoint);
        target = new SigV4Signer(mockXrayConfig);

        // run
        final SdkHttpFullRequest signedRequest = target.signRequest(PAYLOAD);

        // assert
        assertNotNull(signedRequest);
        assertEquals("POST", signedRequest.method().name());
        assertTrue(signedRequest.headers().containsKey("Authorization"));
        assertEquals("application/x-protobuf", signedRequest.firstMatchingHeader("Content-Type").orElse(null));
        assertTrue(signedRequest.getUri().toString().contains(endpoint));
    }

    @Test
    void testSignRequest_withFallbackRegion_whenRegionNotSet() {
        // setup
        System.setProperty("aws.region", Region.US_WEST_2.toString());
        when(mockXrayConfig.getAwsRegion()).thenReturn(null);
        target = new SigV4Signer(mockXrayConfig);

        // run
        final SdkHttpFullRequest signedRequest = target.signRequest(PAYLOAD);

        // assert
        assertNotNull(signedRequest);
        assertEquals("POST", signedRequest.method().name());
        assertTrue(signedRequest.headers().containsKey("Authorization"));
        assertEquals("application/x-protobuf", signedRequest.firstMatchingHeader("Content-Type").orElse(null));

        final String expectedRegion = DefaultAwsRegionProviderChain.builder().build().getRegion().id();
        assertTrue(signedRequest.getUri().toString().contains(String.format("https://xray.%s.amazonaws.com/v1/traces", expectedRegion)));
    }

    @Test
    void testSignRequest_withFallbackStsRole_whenStsRoleNotSet() {
        // setup
        when(mockXrayConfig.getAwsRegion()).thenReturn(Region.US_WEST_2);
        target = new SigV4Signer(mockXrayConfig);

        // run
        final SdkHttpFullRequest signedRequest = target.signRequest(PAYLOAD);

        // assert
        assertNotNull(signedRequest);
        assertEquals("POST", signedRequest.method().name());
        assertTrue(signedRequest.headers().containsKey("Authorization"));
        assertEquals("application/x-protobuf", signedRequest.firstMatchingHeader("Content-Type").orElse(null));
        assertTrue(signedRequest.getUri().toString().contains(String.format("https://xray.%s.amazonaws.com/v1/traces", Region.US_WEST_2.toString())));
    }

    @Test
    void testSignRequest_withCustomCredentials_usingDefaultStsClientFallback() {
        when(mockXrayConfig.getAwsRegion()).thenReturn(Region.US_WEST_2);
        when(mockXrayConfig.getStsRoleArn()).thenReturn("arn:aws:iam::123456789012:role/test-role");

        // Use mocked static builder to simulate StsClient.builder()
        try (MockedStatic<StsClient> mockedStsClientStatic = mockStatic(StsClient.class)) {
            final StsClient mockStsClient = mock(StsClient.class);

            // Setup fake STS response
            when(mockStsClient.assumeRole(any(AssumeRoleRequest.class)))
                    .thenReturn(AssumeRoleResponse.builder()
                            .credentials(Credentials.builder()
                                    .accessKeyId("fake-access")
                                    .secretAccessKey("fake-secret")
                                    .sessionToken("fake-token")
                                    .expiration(Instant.now().plusSeconds(3600))
                                    .build())
                            .build());

            // Mock StsClient builder
            final StsClientBuilder mockBuilder = mock(StsClientBuilder.class);
            when(mockBuilder.region(Region.US_WEST_2)).thenReturn(mockBuilder);
            when(mockBuilder.build()).thenReturn(mockStsClient);

            // Mock the static method StsClient.builder()
            mockedStsClientStatic.when(StsClient::builder).thenReturn(mockBuilder);

            // run
            target = new SigV4Signer(mockXrayConfig, null);
            SdkHttpFullRequest signedRequest = target.signRequest(PAYLOAD);

            // assert
            assertNotNull(signedRequest);
            assertTrue(signedRequest.headers().containsKey("Authorization"));
            assertTrue(signedRequest.getUri().toString().contains(String.format("https://xray.%s.amazonaws.com/v1/traces", Region.US_WEST_2.toString())));
        }
    }

    @Test
    void testSignRequest_withCustomCredentials_usingMockedSts() {
        // setup
        final String expectedRoleArn = "arn:aws:iam::123456789012:role/test-role";
        final String expectedExternalId = "external-id-123";
        when(mockXrayConfig.getAwsRegion()).thenReturn(Region.US_WEST_2);
        when(mockXrayConfig.getStsRoleArn()).thenReturn(expectedRoleArn);
        when(mockXrayConfig.getStsExternalId()).thenReturn(expectedExternalId);

        final StsClient mockStsClient = mock(StsClient.class);
        when(mockStsClient.assumeRole(any(AssumeRoleRequest.class))).thenReturn(
                AssumeRoleResponse.builder()
                        .credentials(Credentials.builder()
                                .accessKeyId("fake-access-key")
                                .secretAccessKey("fake-secret-key")
                                .sessionToken("fake-session-token")
                                .expiration(Instant.now().plusSeconds(3600))
                                .build())
                        .build()
        );

        target = new SigV4Signer(mockXrayConfig, mockStsClient);

        // run
        final SdkHttpFullRequest signedRequest = target.signRequest(PAYLOAD);

        // assert
        assertNotNull(signedRequest);
        assertEquals("POST", signedRequest.method().name());
        assertTrue(signedRequest.headers().containsKey("Authorization"));
        assertEquals("application/x-protobuf", signedRequest.firstMatchingHeader("Content-Type").orElse(null));
        assertTrue(signedRequest.getUri().toString().contains("https://xray.us-west-2.amazonaws.com/v1/traces"));

        ArgumentMatcher<AssumeRoleRequest> matcher = request ->
                expectedRoleArn.equals(request.roleArn()) &&
                        expectedExternalId.equals(request.externalId());

        verify(mockStsClient).assumeRole(argThat(matcher));
    }
}
