/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.model.metric;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JacksonHistogramTest {

    private static final Map<String, Object> TEST_ATTRIBUTES = ImmutableMap.of(
            "key1", new Date().getTime(),
            "key2", UUID.randomUUID().toString());
    private static final String TEST_SERVICE_NAME = "service";
    private static final String TEST_NAME = "name";
    private static final String TEST_DESCRIPTION = "description";
    private static final String TEST_UNIT_NAME = "unit";
    private static final String TEST_START_TIME = UUID.randomUUID().toString();
    private static final String TEST_TIME = UUID.randomUUID().toString();
    private static final String TEST_EVENT_KIND = Metric.KIND.HISTOGRAM.name();
    private static final Double TEST_SUM = 1D;
    private static final List<Bucket> TEST_BUCKETS = Arrays.asList(
            new DefaultBucket(0.0, 5.0, 2L),
            new DefaultBucket(5.0, 10.0, 5L)
    );
    private static final Integer TEST_BUCKETS_COUNT = 2;
    private static final Long TEST_COUNT = 2L;
    private static final Integer TEST_EXPLICIT_BOUNDS_COUNT = 2;
    private static final String TEST_AGGREGATION_TEMPORALITY = "AGGREGATIONTEMPORALITY";

    private JacksonHistogram histogram;

    private JacksonHistogram.Builder builder;

    @BeforeEach
    public void setup() {
        builder = JacksonHistogram.builder()
                .withAttributes(TEST_ATTRIBUTES)
                .withName(TEST_NAME)
                .withDescription(TEST_DESCRIPTION)
                .withEventKind(TEST_EVENT_KIND)
                .withStartTime(TEST_START_TIME)
                .withTime(TEST_TIME)
                .withUnit(TEST_UNIT_NAME)
                .withServiceName(TEST_SERVICE_NAME)
                .withSum(TEST_SUM)
                .withCount(TEST_COUNT)
                .withBucketCount(TEST_BUCKETS_COUNT)
                .withBuckets(TEST_BUCKETS)
                .withExplicitBoundsCount(TEST_EXPLICIT_BOUNDS_COUNT)
                .withAggregationTemporality(TEST_AGGREGATION_TEMPORALITY);

        histogram = builder.build();
    }

    @Test
    public void testGetAttributes() {
        final Map<String, Object> attributes = histogram.getAttributes();
        TEST_ATTRIBUTES.keySet().forEach(key -> {
                    assertThat(attributes, hasKey(key));
                    assertThat(attributes.get(key), is(equalTo(TEST_ATTRIBUTES.get(key))));
                }
        );
    }

    @Test
    public void testGetName() {
        final String name = histogram.getName();
        assertThat(name, is(equalTo(TEST_NAME)));
    }

    @Test
    public void testGetDescription() {
        final String description = histogram.getDescription();
        assertThat(description, is(equalTo(TEST_DESCRIPTION)));
    }

    @Test
    public void testGetKind() {
        final String kind = histogram.getKind();
        assertThat(kind, is(equalTo(TEST_EVENT_KIND)));
    }

    @Test
    public void testGetSum() {
        final Double sum = histogram.getSum();
        assertThat(sum, is(equalTo(TEST_SUM)));
    }

    @Test
    public void testGetCount() {
        final Long count = histogram.getCount();
        assertThat(count, is(equalTo(TEST_COUNT)));
    }

    @Test
    public void testGetServiceName() {
        final String name = histogram.getServiceName();
        assertThat(name, is(equalTo(TEST_SERVICE_NAME)));
    }

    @Test
    public void testGetBuckets() {
        final List<? extends Bucket> buckets = histogram.getBuckets();
        assertThat(buckets.size(), is(equalTo(2)));
        Bucket firstBucket = buckets.get(0);
        Bucket secondBucket = buckets.get(1);

        assertThat(firstBucket.getMin(), is(equalTo(0.0)));
        assertThat(firstBucket.getMax(), is(equalTo(5.0)));
        assertThat(firstBucket.getCount(), is(equalTo(2L)));

        assertThat(secondBucket.getMin(), is(equalTo(5.0)));
        assertThat(secondBucket.getMax(), is(equalTo(10.0)));
        assertThat(secondBucket.getCount(), is(equalTo(5L)));

    }

    @Test
    public void testGetBucketCount() {
        final Integer bucketCount = histogram.getBucketCount();
        assertThat(bucketCount, is(equalTo(TEST_BUCKETS_COUNT)));
    }

    @Test
    public void testGetExplicitBoundsCount() {
        final Integer explicitBoundsCount = histogram.getExplicitBoundsCount();
        assertThat(explicitBoundsCount, is(equalTo(TEST_EXPLICIT_BOUNDS_COUNT)));
    }

    @Test
    public void testGetAggregationTemporality() {
        final String aggregationTemporality = histogram.getAggregationTemporality();
        assertThat(aggregationTemporality, is(equalTo(TEST_AGGREGATION_TEMPORALITY)));
    }

    @Test
    public void testBuilder_missingNonNullParameters_throwsNullPointerException() {
        final JacksonSum.Builder builder = JacksonSum.builder();
        builder.withValue(null);
        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    public void testBuilder_withEmptyTime_throwsIllegalArgumentException() {
        builder.withTime("");
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    public void testGetAttributes_withNull_mustBeEmpty() {
        builder.withAttributes(null);
        JacksonHistogram histogram = builder.build();
        histogram.toJsonString();
        assertThat(histogram.getAttributes(),is(anEmptyMap()));
    }
}