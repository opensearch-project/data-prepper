/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3DataSelection;
import org.opensearch.dataprepper.plugins.source.s3.ownership.BucketOwnerProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

class S3MetricsIT {

    private static final int TIMEOUT_IN_MILLIS = 200;

    private S3Client s3Client;
    private String s3Bucket;
    private Buffer<Record<Event>> buffer;
    private S3ObjectPluginMetrics s3ObjectPluginMetrics;
    private BucketOwnerProvider bucketOwnerProvider;
    private EventMetadataModifier eventMetadataModifier;
    private PrometheusMeterRegistry prometheusMeterRegistry;

    @BeforeEach
    void setUp() {
        prometheusMeterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        Metrics.addRegistry(prometheusMeterRegistry);
        
        s3Bucket = System.getProperty("tests.s3source.bucket");
        s3Client = S3Client.builder()
                .region(Region.of(System.getProperty("tests.s3source.region")))
                .build();

        buffer = mock(Buffer.class);
        eventMetadataModifier = new EventMetadataModifier(S3SourceConfig.DEFAULT_METADATA_ROOT_KEY, false);
        bucketOwnerProvider = b -> Optional.empty();

        PluginMetrics pluginMetrics = PluginMetrics.fromNames("s3-source", "test-pipeline");
        s3ObjectPluginMetrics = new S3ObjectPluginMetrics(pluginMetrics);
    }

    @AfterEach
    void tearDown() {
        Metrics.removeRegistry(prometheusMeterRegistry);
        prometheusMeterRegistry.close();
    }

    @Test
    void testS3WorkerFailureEmitsPrometheusMetrics() {
        String nonExistentKey = "non-existent-key/" + UUID.randomUUID() + "_" + Instant.now().toString() + ".json";
        S3ObjectReference s3ObjectReference = S3ObjectReference.bucketAndKey(s3Bucket, nonExistentKey).build();
        S3ObjectWorker objectWorker = createS3Worker();

        try {
            objectWorker.processS3Object(s3ObjectReference, S3DataSelection.DATA_AND_METADATA, null, null, null);
        } catch (Exception ignored) {
        }

        String prometheusMetrics = prometheusMeterRegistry.scrape();
        
        assertThat("Failed metric should be incremented", getMetricValue(prometheusMetrics, "test_pipeline_s3_source_s3ObjectsFailed_total"), equalTo(1.0));
        assertThat("Succeeded metric should be zero", getMetricValue(prometheusMetrics, "test_pipeline_s3_source_s3ObjectsSucceeded_total"), equalTo(0.0));
        assertThat("Throttled metric should be zero", getMetricValue(prometheusMetrics, "test_pipeline_s3_source_s3ObjectsThrottled_total"), equalTo(0.0));
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

    private S3ObjectWorker createS3Worker() {
        final S3ObjectRequest request = new S3ObjectRequest.Builder(buffer, 100,
                Duration.ofMillis(TIMEOUT_IN_MILLIS), s3ObjectPluginMetrics)
                .bucketOwnerProvider(bucketOwnerProvider)
                .s3Client(s3Client)
                .build();

        return new S3ObjectWorker(request);
    }
}
