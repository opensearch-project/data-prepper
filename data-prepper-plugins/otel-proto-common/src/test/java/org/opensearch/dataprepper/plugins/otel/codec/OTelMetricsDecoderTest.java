/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.otel.codec;

import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

import com.google.protobuf.util.JsonFormat;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.model.metric.JacksonMetric;
import org.opensearch.dataprepper.model.metric.JacksonGauge;
import org.opensearch.dataprepper.model.metric.JacksonSum;
import org.opensearch.dataprepper.model.metric.JacksonHistogram;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class OTelMetricsDecoderTest {
    private static final String TEST_REQUEST_METRICS_FILE = "test-request-multiple-metrics.json";
    
    public OTelMetricDecoder createObjectUnderTest(OTelOutputFormat outputFormat) {
        return new OTelMetricDecoder(outputFormat);
    }

    private String getFileAsJsonString(String requestJsonFileName) throws IOException {
        final StringBuilder jsonBuilder = new StringBuilder();
        try (final InputStream inputStream = Objects.requireNonNull(
                OTelMetricsDecoderTest.class.getClassLoader().getResourceAsStream(requestJsonFileName))) {
            final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            bufferedReader.lines().forEach(jsonBuilder::append);
        }
        return jsonBuilder.toString();
    }

    private ExportMetricsServiceRequest buildExportMetricsServiceRequestFromJsonFile(String requestJsonFileName) throws IOException {
        final ExportMetricsServiceRequest.Builder builder = ExportMetricsServiceRequest.newBuilder();
        JsonFormat.parser().merge(getFileAsJsonString(requestJsonFileName), builder);
        return builder.build();
    }

    private void validateMetric(Event event) {
        JacksonMetric metric = (JacksonMetric)event;
        String kind = metric.getKind();
        assertTrue(kind.equals(Metric.KIND.GAUGE.toString()) || kind.equals(Metric.KIND.SUM.toString()) || kind.equals(Metric.KIND.HISTOGRAM.toString()));
        if (metric.getKind().equals(Metric.KIND.GAUGE.toString())) {
            assertThat(metric.getUnit(), equalTo("1"));
            assertThat(metric.getName(), equalTo("counter-int"));
            JacksonGauge gauge = (JacksonGauge)metric;
            assertThat(gauge.getValue(), equalTo(123.0));
        } else if (metric.getKind().equals(Metric.KIND.SUM.toString())) {
            assertThat(metric.getUnit(), equalTo("1"));
            assertThat(metric.getName(), equalTo("sum-int"));
            JacksonSum sum = (JacksonSum)metric;
            assertThat(sum.getValue(), equalTo(456.0));
        } else { // Histogram
            assertThat(metric.getUnit(), equalTo("1"));
            assertThat(metric.getName(), equalTo("histogram-int"));
            JacksonHistogram histogram = (JacksonHistogram)metric;
            assertThat(histogram.getSum(), equalTo(100.0));
            assertThat(histogram.getCount(), equalTo(30L));
            assertThat(histogram.getExemplars(), equalTo(Collections.emptyList()));
            assertThat(histogram.getExplicitBoundsList(), equalTo(List.of(1.0, 2.0, 3.0, 4.0)));
            assertThat(histogram.getExplicitBoundsCount(), equalTo(4));
            assertThat(histogram.getBucketCountsList(), equalTo(List.of(3L, 5L, 15L, 6L, 1L)));
            assertThat(histogram.getBucketCount(), equalTo(5));
            assertThat(histogram.getAggregationTemporality(), equalTo("AGGREGATION_TEMPORALITY_CUMULATIVE"));
        }
    }

    @Test
    public void testParse() throws Exception {
        final ExportMetricsServiceRequest request = buildExportMetricsServiceRequestFromJsonFile(TEST_REQUEST_METRICS_FILE);
        InputStream inputStream = new ByteArrayInputStream((byte[])request.toByteArray());
        createObjectUnderTest(OTelOutputFormat.OPENSEARCH).parse(inputStream, Instant.now(), (record) -> {
            validateMetric((Event)record.getData());
        });
        
    }
}


