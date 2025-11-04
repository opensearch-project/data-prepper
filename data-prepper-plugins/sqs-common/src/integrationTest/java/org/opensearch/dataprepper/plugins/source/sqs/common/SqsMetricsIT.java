/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.sqs.common;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.core.meter.EMFLoggingMeterRegistry;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ListQueuesRequest;

import static org.junit.jupiter.api.Assertions.assertTrue;

class SqsMetricsIT {

    private MeterRegistry meterRegistry;

    @BeforeEach
    void setUp() {
        meterRegistry = new EMFLoggingMeterRegistry();
        Metrics.globalRegistry.clear();
        Metrics.addRegistry(meterRegistry);
    }

    @Test
    void testSqsClientFactoryGeneratesMetrics() throws InterruptedException {
        final SqsClient sqsClient = SqsClientFactory.createSqsClient(
                Region.US_EAST_1, 
                DefaultCredentialsProvider.create()
        );

        try {
            sqsClient.listQueues(ListQueuesRequest.builder().build());
        } catch (Exception ignored) {
        }

        // Wait for EMF registry to publish metrics
        Thread.sleep(2000);

        assertTrue(meterRegistry.getMeters().stream()
                .anyMatch(meter -> meter.getId().getName().startsWith("aws.sqs.")),
                "Expected SQS metrics to be generated");

        sqsClient.close();
    }
}
