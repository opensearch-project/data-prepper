/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.sqs;

import com.linecorp.armeria.client.retry.Backoff;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.plugins.source.sqs.common.SqsWorkerCommon;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

class SqsMetricsIT {
    private SqsClient sqsClient;
    private SqsWorkerCommon sqsWorkerCommon;
    private PrometheusMeterRegistry prometheusMeterRegistry;

    @BeforeEach
    void setUp() {
        prometheusMeterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        Metrics.addRegistry(prometheusMeterRegistry);

        sqsClient = SqsClient.builder()
                .region(Region.US_EAST_1)
                .build();

        final Backoff backoff = mock(Backoff.class);
        final AcknowledgementSetManager acknowledgementSetManager = mock(AcknowledgementSetManager.class);
        PluginMetrics pluginMetrics = PluginMetrics.fromNames("sqs_source", "test_pipeline");
        sqsWorkerCommon = new SqsWorkerCommon(backoff, pluginMetrics, acknowledgementSetManager);
    }

    @AfterEach
    void tearDown() {
        Metrics.removeRegistry(prometheusMeterRegistry);
        prometheusMeterRegistry.close();
    }

    @Test
    void testSqsWorkerFailureEmitsPrometheusMetrics() {
        String nonExistentQueueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/non-existent-queue";
        
        try {
            sqsWorkerCommon.pollSqsMessages(nonExistentQueueUrl, sqsClient, 10, null, null);
        } catch (Exception ignored) {
        }

        String prometheusMetrics = prometheusMeterRegistry.scrape();
        
        // Should increment queue not found metric for non-existent queue
        assertThat("Queue not found metric should be incremented", 
                getMetricValue(prometheusMetrics, "test_pipeline_sqs_source_sqsQueueNotFound_total"), equalTo(1.0));
        assertThat("Access denied metric should be zero", 
                getMetricValue(prometheusMetrics, "test_pipeline_sqs_source_sqsMessagesAccessDenied_total"), equalTo(0.0));
        assertThat("Throttled metric should be zero", 
                getMetricValue(prometheusMetrics, "test_pipeline_sqs_source_sqsMessagesThrottled_total"), equalTo(0.0));
    }

    private double getMetricValue(String prometheusMetrics, String metricName) {
        String[] lines = prometheusMetrics.split("\n");
        for (String line : lines) {
            if (line.startsWith(metricName + " ")) {
                return Double.parseDouble(line.split(" ")[1]);
            }
        }
        return 0.0;
    }
}
