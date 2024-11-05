/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.otelmetrics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogram;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.resource.v1.Resource;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.metric.Bucket;
import org.opensearch.dataprepper.model.metric.DefaultBucket;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MetricsPluginExponentialHistogramTest {

    private static final Double MAX_ERROR = 0.00001;

    private OTelMetricsRawProcessor rawProcessor;

    @Mock
    private OtelMetricsRawProcessorConfig config;

    private static final ExponentialHistogramDataPoint EXPONENTIAL_HISTOGRAM_DATA_POINT = ExponentialHistogramDataPoint.newBuilder()
            .setNegative(ExponentialHistogramDataPoint.Buckets.newBuilder()
                    .addBucketCounts(1)
                    .addBucketCounts(2)
                    .addBucketCounts(3).setOffset(0).build())
            .setPositive(ExponentialHistogramDataPoint.Buckets.newBuilder()
                    .addBucketCounts(4)
                    .addBucketCounts(5)
                    .addBucketCounts(6).setOffset(1).build())
            .setScale(3)
            .setCount(4)
            .setSum(1d / 3d)
            .setFlags(1)
            .build();

    @BeforeEach
    void init() {
        PluginSetting testsettings = new PluginSetting("testsettings", Collections.emptyMap());
        testsettings.setPipelineName("testpipeline");
        rawProcessor = new OTelMetricsRawProcessor(testsettings, config);
    }

    @Test
    void testWithMaxScaleExceedingConfiguredNegativeScale() {
        when(config.getExponentialHistogramMaxAllowedScale()).thenReturn(-2);
        lenient().when(config.getCalculateExponentialHistogramBuckets()).thenReturn(true);
        ExponentialHistogram histogram = ExponentialHistogram.newBuilder()
                .addDataPoints(EXPONENTIAL_HISTOGRAM_DATA_POINT).build();

        List<Record<? extends Metric>> processedRecords = (List<Record<? extends Metric>>) rawProcessor.doExecute(Collections.singletonList(new Record<>(fillServiceRequest(histogram))));
        assertThat(processedRecords).isEmpty();
    }

    @Test
    void testWithMaxScaleExceedingConfiguredPositiveScale() {
        when(config.getExponentialHistogramMaxAllowedScale()).thenReturn(2);
        lenient().when(config.getCalculateExponentialHistogramBuckets()).thenReturn(true);
        ExponentialHistogram histogram = ExponentialHistogram.newBuilder()
                .addDataPoints(EXPONENTIAL_HISTOGRAM_DATA_POINT).build();

        List<Record<? extends Metric>> processedRecords = (List<Record<? extends Metric>>) rawProcessor.doExecute(Collections.singletonList(new Record<>(fillServiceRequest(histogram))));
        assertThat(processedRecords).isEmpty();
    }

    @Test
    void test() throws JsonProcessingException {
        when(config.getExponentialHistogramMaxAllowedScale()).thenReturn(10);
        lenient().when(config.getCalculateExponentialHistogramBuckets()).thenReturn(true);
        ExponentialHistogram histogram = ExponentialHistogram.newBuilder()
                .addDataPoints(EXPONENTIAL_HISTOGRAM_DATA_POINT).build();

        List<Record<? extends Metric>> processedRecords = (List<Record<? extends Metric>>) rawProcessor.doExecute(Collections.singletonList(new Record<>(fillServiceRequest(histogram))));
        Record<? extends Metric> record = processedRecords.get(0);
        ObjectMapper objectMapper = new ObjectMapper();
        Map<Object, Object> map = objectMapper.readValue(record.getData().toJsonString(), Map.class);

        DefaultBucket negative_1 = new DefaultBucket(1D, 1.0905077326652577, 1L);
        DefaultBucket negative_2 = new DefaultBucket(1.0905077326652577, 1.189207115002721, 2L);
        DefaultBucket negative_3 = new DefaultBucket(1.189207115002721, 1.2968395546510096, 3L);


        DefaultBucket positive_1 = new DefaultBucket(1.0905077326652577, 1.189207115002721, 4L);
        DefaultBucket positive_2 = new DefaultBucket(1.189207115002721, 1.2968395546510096, 5L);
        DefaultBucket positive_3 = new DefaultBucket(1.2968395546510096, 1.4142135623730951, 6L);

        assertHistogramProcessing(map, Arrays.asList(negative_1, negative_2, negative_3, positive_1, positive_2, positive_3));
    }

    @Test
    void testWithHistogramCalculationFlagDisabled() throws JsonProcessingException {
        when(config.getCalculateExponentialHistogramBuckets()).thenReturn(false);
        lenient().when(config.getExponentialHistogramMaxAllowedScale()).thenReturn(10);

        ExponentialHistogram histogram = ExponentialHistogram.newBuilder()
                .addDataPoints(EXPONENTIAL_HISTOGRAM_DATA_POINT).build();

        List<Record<? extends Metric>> processedRecords = (List<Record<? extends Metric>>) rawProcessor.doExecute(Collections.singletonList(new Record<>(fillServiceRequest(histogram))));
        Record<? extends Metric> record = processedRecords.get(0);
        ObjectMapper objectMapper = new ObjectMapper();
        Map<Object, Object> map = objectMapper.readValue(record.getData().toJsonString(), Map.class);

        assertHistogramProcessing(map, Collections.emptyList());
    }

    private ExportMetricsServiceRequest fillServiceRequest(ExponentialHistogram histogram) {
        io.opentelemetry.proto.metrics.v1.Metric metric = io.opentelemetry.proto.metrics.v1.Metric.newBuilder()
                .setExponentialHistogram(histogram)
                .setUnit("seconds")
                .setName("name")
                .setDescription("description")
                .build();
        ScopeMetrics scopeMetrics = ScopeMetrics.newBuilder()
                .addMetrics(metric).build();

        Resource resource = Resource.newBuilder()
                .addAttributes(KeyValue.newBuilder()
                        .setKey("service.name")
                        .setValue(AnyValue.newBuilder().setStringValue("service").build())
                ).build();
        ResourceMetrics resourceMetrics = ResourceMetrics.newBuilder()
                .setResource(resource)
                .addScopeMetrics(scopeMetrics)
                .build();
        return ExportMetricsServiceRequest.newBuilder().addResourceMetrics(resourceMetrics).build();
    }

    private void assertHistogramProcessing(Map<Object, Object> map, List<DefaultBucket> expectedBuckets) {
        assertThat(map).contains(entry("kind", Metric.KIND.EXPONENTIAL_HISTOGRAM.toString()));
        assertThat(map).contains(entry("unit", "seconds"));
        assertThat(map).contains(entry("description", "description"));
        assertThat(map).contains(entry("name", "name"));
        assertThat(map).contains(entry("sum", (1d / 3d)));
        assertThat(map).contains(entry("count", 4));
        assertThat(map).contains(entry("serviceName", "service"));
        assertThat(map).contains(entry("aggregationTemporality", "AGGREGATION_TEMPORALITY_UNSPECIFIED"));
        assertThat(map).contains(entry("flags", 1));
        assertThat(map).contains(entry("negativeOffset", 0));
        assertThat(map).contains(entry("positiveOffset", 1));
        assertThat(map).contains(entry("positive", Arrays.asList(4, 5, 6)));
        assertThat(map).contains(entry("negative", Arrays.asList(1, 2, 3)));
        assertThat(map).contains(entry("scale", 3));

        if (expectedBuckets.isEmpty()) {
            assertThat(map).doesNotContainKey("negativeBuckets");
            assertThat(map).doesNotContainKey("positiveBuckets");
        } else {
            assertThat(map).containsKey("negativeBuckets");
            assertThat(map).containsKey("positiveBuckets");
            List<Map> negativeBuckets = (List<Map>) map.get("negativeBuckets");
            List<Map> positiveBuckets = (List<Map>) map.get("positiveBuckets");
            negativeBuckets.addAll(positiveBuckets);
            assertThat(negativeBuckets).hasSize(expectedBuckets.size());

            for (int i = 0; i < expectedBuckets.size(); i++) {
                Bucket expectedBucket = expectedBuckets.get(i);
                Map<Object, Object> actualBucket = negativeBuckets.get(i);
                Double min = (Double) actualBucket.get("min");
                Double max = (Double) actualBucket.get("max");
                Integer count = (Integer) actualBucket.get("count");

                MatcherAssert.assertThat(expectedBucket.getMin(), Matchers.closeTo(min, MAX_ERROR));
                MatcherAssert.assertThat(expectedBucket.getMax(), Matchers.closeTo(max, MAX_ERROR));
                assertThat(Integer.toUnsignedLong(count)).isEqualTo(expectedBucket.getCount());
            }
        }
    }
}
