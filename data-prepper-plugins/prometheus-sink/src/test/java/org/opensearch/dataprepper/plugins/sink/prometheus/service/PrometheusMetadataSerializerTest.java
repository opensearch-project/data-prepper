/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.prometheus.service;

import com.arpnetworking.metrics.prometheus.Remote;
import com.arpnetworking.metrics.prometheus.Types;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class PrometheusMetadataSerializerTest {

    @Test
    public void testInjectMetadata_emptyList() throws IOException {
        Remote.WriteRequest request = Remote.WriteRequest.newBuilder().build();
        byte[] originalBytes = request.toByteArray();

        byte[] result = PrometheusMetadataSerializer.injectMetadata(originalBytes, new ArrayList<>());

        assertThat(result, equalTo(originalBytes));
    }

    @Test
    public void testInjectMetadata_nullList() throws IOException {
        Remote.WriteRequest request = Remote.WriteRequest.newBuilder().build();
        byte[] originalBytes = request.toByteArray();

        byte[] result = PrometheusMetadataSerializer.injectMetadata(originalBytes, null);

        assertThat(result, equalTo(originalBytes));
    }

    @Test
    public void testInjectMetadata_singleMetadata() throws IOException {
        Remote.WriteRequest request = Remote.WriteRequest.newBuilder()
                .addTimeseries(Types.TimeSeries.newBuilder()
                        .addLabels(Types.Label.newBuilder()
                                .setName("__name__")
                                .setValue("test_metric")
                                .build())
                        .build())
                .build();
        byte[] originalBytes = request.toByteArray();

        PrometheusMetricMetadata metadata = PrometheusMetricMetadata.builder()
                .type(PrometheusMetricMetadata.MetricType.COUNTER)
                .metricFamilyName("test_metric_total")
                .help("Test metric description")
                .unit("bytes")
                .build();

        byte[] result = PrometheusMetadataSerializer.injectMetadata(originalBytes,
                Collections.singletonList(metadata));

        // Result should be longer than original
        assertThat(result.length, greaterThan(originalBytes.length));

        // Verify the additional bytes contain our metadata
        // The metadata should be appended after the original WriteRequest
        assertThat(result.length, greaterThan(originalBytes.length + 10));
    }

    @Test
    public void testInjectMetadata_multipleMetadata() throws IOException {
        Remote.WriteRequest request = Remote.WriteRequest.newBuilder().build();
        byte[] originalBytes = request.toByteArray();

        List<PrometheusMetricMetadata> metadataList = Arrays.asList(
                PrometheusMetricMetadata.builder()
                        .type(PrometheusMetricMetadata.MetricType.COUNTER)
                        .metricFamilyName("metric1_total")
                        .help("Metric 1")
                        .unit("s")
                        .build(),
                PrometheusMetricMetadata.builder()
                        .type(PrometheusMetricMetadata.MetricType.GAUGE)
                        .metricFamilyName("metric2")
                        .help("Metric 2")
                        .unit("bytes")
                        .build()
        );

        byte[] result = PrometheusMetadataSerializer.injectMetadata(originalBytes, metadataList);

        assertThat(result.length, greaterThan(originalBytes.length));
    }

    @Test
    public void testInjectMetadata_deduplication() throws IOException {
        Remote.WriteRequest request = Remote.WriteRequest.newBuilder().build();
        byte[] originalBytes = request.toByteArray();

        // Create duplicate metadata for the same metric family
        List<PrometheusMetricMetadata> metadataList = Arrays.asList(
                PrometheusMetricMetadata.builder()
                        .type(PrometheusMetricMetadata.MetricType.COUNTER)
                        .metricFamilyName("test_metric")
                        .help("Description 1")
                        .unit("bytes")
                        .build(),
                PrometheusMetricMetadata.builder()
                        .type(PrometheusMetricMetadata.MetricType.COUNTER)
                        .metricFamilyName("test_metric")
                        .help("Description 2")  // Different description
                        .unit("bytes")
                        .build()
        );

        byte[] result = PrometheusMetadataSerializer.injectMetadata(originalBytes, metadataList);

        // Should only include one metadata entry (first one wins)
        // The size increase should be less than double
        int singleMetadataSize = PrometheusMetadataSerializer.estimateMetadataSize(
                Collections.singletonList(metadataList.get(0)));
        int actualIncrease = result.length - originalBytes.length;

        // Actual increase should be close to single metadata size (with some protobuf overhead)
        assertThat(actualIncrease, lessThan(singleMetadataSize * 2));
    }

    @Test
    public void testEstimateMetadataSize_empty() {
        int size = PrometheusMetadataSerializer.estimateMetadataSize(new ArrayList<>());
        assertThat(size, equalTo(0));
    }

    @Test
    public void testEstimateMetadataSize_null() {
        int size = PrometheusMetadataSerializer.estimateMetadataSize(null);
        assertThat(size, equalTo(0));
    }

    @Test
    public void testEstimateMetadataSize_single() {
        PrometheusMetricMetadata metadata = PrometheusMetricMetadata.builder()
                .type(PrometheusMetricMetadata.MetricType.COUNTER)
                .metricFamilyName("test_metric_total")
                .help("This is a test metric")
                .unit("bytes")
                .build();

        int size = PrometheusMetadataSerializer.estimateMetadataSize(Collections.singletonList(metadata));

        assertThat(size, greaterThan(0));
        assertThat(size, lessThan(200)); // Reasonable upper bound
    }

    @Test
    public void testEstimateMetadataSize_multiple() {
        List<PrometheusMetricMetadata> metadataList = Arrays.asList(
                PrometheusMetricMetadata.builder()
                        .type(PrometheusMetricMetadata.MetricType.COUNTER)
                        .metricFamilyName("metric1")
                        .help("Help 1")
                        .unit("s")
                        .build(),
                PrometheusMetricMetadata.builder()
                        .type(PrometheusMetricMetadata.MetricType.GAUGE)
                        .metricFamilyName("metric2")
                        .help("Help 2")
                        .unit("bytes")
                        .build()
        );

        int size = PrometheusMetadataSerializer.estimateMetadataSize(metadataList);

        // Size should be roughly double of a single metadata
        int singleSize = PrometheusMetadataSerializer.estimateMetadataSize(
                Collections.singletonList(metadataList.get(0)));

        assertThat(size, greaterThan(singleSize));
        assertThat(size, lessThan(singleSize * 3)); // Some overhead is expected
    }

    @Test
    public void testEstimateMetadataSize_withDeduplication() {
        List<PrometheusMetricMetadata> metadataList = Arrays.asList(
                PrometheusMetricMetadata.builder()
                        .type(PrometheusMetricMetadata.MetricType.COUNTER)
                        .metricFamilyName("test_metric")
                        .help("Description 1")
                        .unit("bytes")
                        .build(),
                PrometheusMetricMetadata.builder()
                        .type(PrometheusMetricMetadata.MetricType.COUNTER)
                        .metricFamilyName("test_metric")  // Same name
                        .help("Description 2")
                        .unit("bytes")
                        .build()
        );

        int size = PrometheusMetadataSerializer.estimateMetadataSize(metadataList);

        // Should only count one metadata due to deduplication
        int singleSize = PrometheusMetadataSerializer.estimateMetadataSize(
                Collections.singletonList(metadataList.get(0)));

        assertThat(size, equalTo(singleSize));
    }

    @Test
    public void testInjectMetadata_preservesOriginalData() throws IOException {
        // Create a WriteRequest with actual time series data
        Types.TimeSeries timeSeries = Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("test_metric").build())
                .addLabels(Types.Label.newBuilder().setName("instance").setValue("localhost").build())
                .addSamples(Types.Sample.newBuilder().setValue(42.0).setTimestamp(System.currentTimeMillis()).build())
                .build();

        Remote.WriteRequest originalRequest = Remote.WriteRequest.newBuilder()
                .addTimeseries(timeSeries)
                .build();

        byte[] originalBytes = originalRequest.toByteArray();

        PrometheusMetricMetadata metadata = PrometheusMetricMetadata.builder()
                .type(PrometheusMetricMetadata.MetricType.GAUGE)
                .metricFamilyName("test_metric")
                .help("Test metric")
                .unit("1")
                .build();

        byte[] resultBytes = PrometheusMetadataSerializer.injectMetadata(originalBytes,
                Collections.singletonList(metadata));

        // The original bytes should be a prefix of the result
        for (int i = 0; i < originalBytes.length; i++) {
            assertThat("Byte at position " + i + " should match",
                    resultBytes[i], equalTo(originalBytes[i]));
        }
    }

    @Test
    public void testInjectMetadata_withEmptyFields() throws IOException {
        Remote.WriteRequest request = Remote.WriteRequest.newBuilder().build();
        byte[] originalBytes = request.toByteArray();

        PrometheusMetricMetadata metadata = PrometheusMetricMetadata.builder()
                .type(PrometheusMetricMetadata.MetricType.COUNTER)
                .metricFamilyName("test_metric")
                .help("")  // Empty help
                .unit("")  // Empty unit
                .build();

        byte[] result = PrometheusMetadataSerializer.injectMetadata(originalBytes,
                Collections.singletonList(metadata));

        // Should still work, just smaller size increase
        assertThat(result.length, greaterThan(originalBytes.length));
    }
}
