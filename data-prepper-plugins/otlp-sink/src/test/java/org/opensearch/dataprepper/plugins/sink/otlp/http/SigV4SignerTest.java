/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.otlp.http;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.sink.otlp.configuration.OtlpSinkConfig;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.regions.Region;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SigV4SignerTest {

    private static final byte[] PAYLOAD = "test-payload".getBytes(StandardCharsets.UTF_8);
    private static final Region REGION = Region.US_WEST_2;

    private OtlpSinkConfig mockConfig;
    private AwsCredentialsSupplier mockSupplier;
    private SigV4Signer target;

    @BeforeEach
    void setup() {
        mockConfig = mock(OtlpSinkConfig.class);
        mockSupplier = mock(AwsCredentialsSupplier.class);

        when(mockConfig.getAwsRegion()).thenReturn(REGION);

        final AwsBasicCredentials mockCredentials = AwsBasicCredentials.create("mockAccessKey", "mockSecretKey");
        final StaticCredentialsProvider mockCredentialsProvider = StaticCredentialsProvider.create(mockCredentials);
        when(mockSupplier.getProvider(any())).thenReturn(mockCredentialsProvider);
    }

    @Test
    void testSignRequest_withExplicitEndpoint() {
        final String endpoint = "https://performance.us-west-2.xray.cloudwatch.aws.dev/v1/traces";
        when(mockConfig.getEndpoint()).thenReturn(endpoint);

        target = new SigV4Signer(mockSupplier, mockConfig);
        final SdkHttpFullRequest request = target.signRequest(PAYLOAD);

        assertNotNull(request);
        assertEquals("POST", request.method().name());
        assertTrue(request.headers().containsKey("Authorization"));
        assertEquals("application/x-protobuf", request.firstMatchingHeader("Content-Type").orElse(null));
        assertEquals(endpoint, request.getUri().toString());
    }

    @Test
    void testSignRequest_withDefaultEndpoint() {
        when(mockConfig.getEndpoint()).thenReturn(null);

        target = new SigV4Signer(mockSupplier, mockConfig);
        final SdkHttpFullRequest request = target.signRequest(PAYLOAD);

        assertNotNull(request);
        assertEquals("POST", request.method().name());
        assertTrue(request.headers().containsKey("Authorization"));
        assertEquals("application/x-protobuf", request.firstMatchingHeader("Content-Type").orElse(null));
        assertEquals("https://xray.us-west-2.amazonaws.com/v1/traces", request.getUri().toString());
    }
}