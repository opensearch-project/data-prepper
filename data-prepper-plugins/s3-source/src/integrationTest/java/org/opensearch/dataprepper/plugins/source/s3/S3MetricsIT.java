/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.s3;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.core.meter.EMFLoggingMeterRegistry;
import org.opensearch.dataprepper.plugins.source.s3.configuration.AwsAuthenticationOptions;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class S3MetricsIT {

    private MeterRegistry meterRegistry;

    @BeforeEach
    void setUp() {
        meterRegistry = new EMFLoggingMeterRegistry();
        Metrics.globalRegistry.clear();
        Metrics.addRegistry(meterRegistry);
    }

    @Test
    void testS3ClientBuilderFactoryGeneratesMetrics() throws InterruptedException {
        final S3SourceConfig s3SourceConfig = mock(S3SourceConfig.class);
        final AwsAuthenticationOptions awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);
        when(s3SourceConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.US_EAST_1);
        
        final S3ClientBuilderFactory factory = new S3ClientBuilderFactory(s3SourceConfig, DefaultCredentialsProvider.create());
        final S3Client s3Client = factory.getS3Client();

        try {
            s3Client.listBuckets(ListBucketsRequest.builder().build());
        } catch (Exception ignored) {
        }

        // Wait for EMF registry to publish metrics
        Thread.sleep(2000);

        assertTrue(meterRegistry.getMeters().stream()
                .anyMatch(meter -> meter.getId().getName().startsWith("aws.s3.")),
                "Expected S3 metrics to be generated");

        s3Client.close();
    }
}
