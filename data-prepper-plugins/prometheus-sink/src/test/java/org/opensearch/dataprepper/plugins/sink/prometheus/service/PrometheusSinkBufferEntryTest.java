/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.prometheus.service;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.metric.JacksonSum;
import org.opensearch.dataprepper.model.metric.JacksonSummary;
import org.opensearch.dataprepper.model.metric.JacksonGauge;
import org.opensearch.dataprepper.model.metric.JacksonHistogram;
import org.opensearch.dataprepper.model.metric.JacksonExponentialHistogram;
import org.opensearch.dataprepper.model.metric.DefaultBucket;
import org.opensearch.dataprepper.model.metric.Bucket;
import org.opensearch.dataprepper.model.metric.Quantile;
import org.opensearch.dataprepper.model.metric.DefaultQuantile;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.MatcherAssert.assertThat;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class PrometheusSinkBufferEntryTest {
    private Event event;

    private PrometheusSinkBufferEntry prometheusSinkBufferEntry;

    PrometheusSinkBufferEntry createObjectUnderTest(Event event)throws Exception {
        return new PrometheusSinkBufferEntry(event, true);
    }

    @ParameterizedTest
    @MethodSource("createDifferentMetricEvents")
    public void testGaugeEvent() throws Exception {
        event = createGaugeEvent();
        prometheusSinkBufferEntry = createObjectUnderTest(event);
        assertThat(prometheusSinkBufferEntry.getTimeSeries(), notNullValue());
        assertThat(prometheusSinkBufferEntry.getEvent(), sameInstance(event));
        assertThat(prometheusSinkBufferEntry.getEstimatedSize(), greaterThan(1L));
        assertThat(prometheusSinkBufferEntry.exceedsMaxEventSizeThreshold(), equalTo(false));
    }

    @Test
    public void testNonMetricEvent() throws Exception {
        event = JacksonEvent.builder()
                .withEventType("event")
                .build();
        assertThrows(RuntimeException.class, () -> createObjectUnderTest(event));
    }

    @Test
    public void testInvalidMetricEvent() throws Exception {
        event = JacksonEvent.builder()
                .withEventType("metric")
                .build();
        assertThrows(RuntimeException.class, () -> createObjectUnderTest(event));
    }

    static Stream<Event> createDifferentMetricEvents() {
        return Stream.of(
            createGaugeEvent(),
            createSumEvent(),
            createSummaryEvent(),
            createHistogramEvent(),
            createExponentialHistogramEvent()
        );
    }

    private static Event createGaugeEvent() {
        return JacksonGauge.builder()
            .withName("gauge")
            .withDescription("Test Gauge Metric")
            .withTimeReceived(Instant.now())
            .withTime("2025-09-27T18:00:00Z")
            .withStartTime("2025-09-27T17:00:00Z")
            .withUnit("1")
            .withValue(1.0d)
            .build(false);
    }

    private static Event createSumEvent() {
        return JacksonSum.builder()
            .withName("sum")
            .withDescription("Test Sum Metric")
            .withTimeReceived(Instant.now())
            .withTime("2025-09-27T18:00:00Z")
            .withStartTime("2025-09-27T17:00:00Z")
            .withIsMonotonic(true)
            .withUnit("1")
            .withAggregationTemporality("cumulative")
            .withValue(1.0d)
            .build(false);
    }

    private static Event createSummaryEvent() {
        List<Quantile> quantiles = Arrays.asList(
            new DefaultQuantile(0.5d, 10d),
            new DefaultQuantile(0.75d, 20d),
            new DefaultQuantile(0.9d, 30d),
            new DefaultQuantile(0.99d, 5d)
        );
        return JacksonSummary.builder()
            .withName("summary")
            .withDescription("Test Summary Metric")
            .withTimeReceived(Instant.now())
            .withTime("2025-09-27T18:00:00Z")
            .withStartTime("2025-09-27T17:00:00Z")
            .withUnit("1")
            .withSum(1)
            .withCount(2L)
            .withQuantilesValueCount(4)
            .withQuantiles(quantiles)
            .build(false);
    }

    private static Event createHistogramEvent() {
        final List<Bucket> TEST_BUCKETS = Arrays.asList(
                new DefaultBucket(Double.NEGATIVE_INFINITY, 5.0, 2222L),
                new DefaultBucket(5.0, 10.0, 5555L),
                new DefaultBucket(10.0, 100.0, 3333L),
                new DefaultBucket(100.0, Double.POSITIVE_INFINITY, 7777L)
        );
        final List<Long> TEST_BUCKET_COUNTS_LIST = Arrays.asList(2222L, 5555L, 3333L, 7777L);
        final List<Double> TEST_EXPLICIT_BOUNDS_LIST = Arrays.asList(5D, 10D, 100D);
        return JacksonHistogram.builder()
            .withName("histogram")
            .withDescription("Test Histogram Metric")
            .withTimeReceived(Instant.now())
            .withTime("2025-09-27T18:00:00Z")
            .withStartTime("2025-09-27T17:00:00Z")
            .withUnit("1")
            .withSum(1)
            .withMin(2.0d)
            .withMax(3.0d)
            .withCount(10L)
            .withBucketCount(TEST_BUCKETS.size())
            .withExplicitBoundsCount(TEST_EXPLICIT_BOUNDS_LIST.size())
            .withAggregationTemporality("cumulative")
            .withBuckets(TEST_BUCKETS)
            .withBucketCountsList(TEST_BUCKET_COUNTS_LIST)
            .withExplicitBoundsList(TEST_EXPLICIT_BOUNDS_LIST)
            .build(false);
    }

    private static Event createExponentialHistogramEvent() {
        final List<Long> TEST_POSITIVE_COUNTS = Arrays.asList(1L, 3L, 5L);
        final List<Long> TEST_NEGATIVE_COUNTS = Arrays.asList(4L, 8L, 2L, 6L);
        return JacksonExponentialHistogram.builder()
            .withName("exponentialHistogram")
            .withDescription("Test Exponential Histogram Metric")
            .withTimeReceived(Instant.now())
            .withTime("2025-09-27T18:00:00Z")
            .withStartTime("2025-09-27T17:00:00Z")
            .withUnit("1")
            .withSum(1)
            .withCount(10L)
            .withScale(0)
            .withPositiveOffset(-1)
            .withNegativeOffset(2)
            .withZeroCount(3)
            .withAggregationTemporality("cumulative")
            .withPositive(TEST_POSITIVE_COUNTS)
            .withNegative(TEST_NEGATIVE_COUNTS)
            .build(false);
    }
}

