/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.metric;

import com.google.common.collect.ImmutableMap;
import io.micrometer.core.instrument.util.IOUtils;
import org.json.JSONException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.TestObject;
import org.skyscreamer.jsonassert.JSONAssert;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class JacksonExponentialHistogramTest {

    private static final Long TEST_KEY1_TIME = TimeUnit.MILLISECONDS.toNanos(ZonedDateTime.of(
            LocalDateTime.of(2020, 5, 24, 14, 0, 0),
            ZoneOffset.UTC).toInstant().toEpochMilli());

    private static final String TEST_KEY2 = UUID.randomUUID().toString();

    private static final Map<String, Object> TEST_ATTRIBUTES = ImmutableMap.of(
            "key1", TEST_KEY1_TIME,
            "key2", TEST_KEY2);
    private static final String TEST_SERVICE_NAME = "service";
    private static final String TEST_NAME = "name";
    private static final String TEST_DESCRIPTION = "description";
    private static final String TEST_UNIT_NAME = "unit";
    private static final String TEST_START_TIME = UUID.randomUUID().toString();
    private static final String TEST_TIME = UUID.randomUUID().toString();
    private static final String TEST_EVENT_KIND = Metric.KIND.EXPONENTIAL_HISTOGRAM.name();
    private static final Double TEST_SUM = 1D;
    private static final List<Bucket> TEST_POSITIVE_BUCKETS = Arrays.asList(
            new DefaultBucket(0.0, 5.0, 2L),
            new DefaultBucket(5.0, 10.0, 5L)
    );

    private static final List<Bucket> TEST_NEGATIVE_BUCKETS = Arrays.asList(
            new DefaultBucket(0.0, 5.0, 2L),
            new DefaultBucket(5.0, 10.0, 5L)
    );
    private static final List<Long> TEST_NEGATIVE = Arrays.asList(1L, 2L, 3L);
    private static final List<Long> TEST_POSITIVE = Arrays.asList(4L, 5L);
    private static final Long TEST_COUNT = 2L;
    private static final String TEST_AGGREGATION_TEMPORALITY = "AGGREGATIONTEMPORALITY";
    private static final String TEST_SCHEMA_URL = "schema";
    private static final Integer TEST_SCALE = -3;
    private static final Long TEST_ZERO_COUNT = 1L;
    private static final Integer TEST_NEGATIVE_OFFSET = 2;
    private static final Integer TEST_POSITIVE_OFFSET = 5;

    private JacksonExponentialHistogram histogram;

    private JacksonExponentialHistogram.Builder builder;

    @BeforeEach
    public void setup() {
        builder = JacksonExponentialHistogram.builder()
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
                .withNegativeBuckets(TEST_NEGATIVE_BUCKETS)
                .withPositiveBuckets(TEST_POSITIVE_BUCKETS)
                .withAggregationTemporality(TEST_AGGREGATION_TEMPORALITY)
                .withSchemaUrl(TEST_SCHEMA_URL)
                .withScale(TEST_SCALE)
                .withZeroCount(TEST_ZERO_COUNT)
                .withPositiveOffset(TEST_POSITIVE_OFFSET)
                .withNegativeOffset(TEST_NEGATIVE_OFFSET)
                .withNegative(TEST_NEGATIVE)
                .withPositive(TEST_POSITIVE);

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
    public void testGetScale() {
        Integer scale = histogram.getScale();
        assertThat(scale, is(equalTo(TEST_SCALE)));
    }

    @Test
    public void testZeroCount() {
        Long zeroCount = histogram.getZeroCount();
        assertThat(zeroCount, is(equalTo(TEST_ZERO_COUNT)));
    }

    @Test
    public void testGetNegativeBuckets() {
        final List<? extends Bucket> buckets = histogram.getNegativeBuckets();
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
    public void testGetPositiveBuckets() {
        final List<? extends Bucket> buckets = histogram.getPositiveBuckets();
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
    public void testGetNegative() {
        List<Long> negativeBucketCounts = histogram.getNegative();
        assertThat(negativeBucketCounts.size(), is(equalTo(3)));
        assertEquals(negativeBucketCounts, TEST_NEGATIVE);
    }

    @Test
    public void testGetPositive() {
        List<Long> negativeBucketCounts = histogram.getPositive();
        assertThat(negativeBucketCounts.size(), is(equalTo(2)));
        assertEquals(negativeBucketCounts, TEST_POSITIVE);
    }

    @Test
    public void testGetPositiveOffset() {
        Integer positiveOffset = histogram.getPositiveOffset();
        assertThat(positiveOffset, is(TEST_POSITIVE_OFFSET));
    }

    @Test
    public void testGetNegativeOffset() {
        Integer negativeOffset = histogram.getNegativeOffset();
        assertThat(negativeOffset, is(TEST_NEGATIVE_OFFSET));
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
        JacksonExponentialHistogram histogram = builder.build();
        histogram.toJsonString();
        assertThat(histogram.getAttributes(), is(anEmptyMap()));
    }

    @Test
    public void testHistogramToJsonString() throws JSONException {
        histogram.put("foo", "bar");
        final String value = UUID.randomUUID().toString();
        histogram.put("testObject", new TestObject(value));
        histogram.put("list", Arrays.asList(1, 4, 5));
        final String result = histogram.toJsonString();

        String file = IOUtils.toString(this.getClass().getResourceAsStream("/testjson/exponentialHistogram.json"));
        String expected = String.format(file, TEST_START_TIME, TEST_TIME, value, TEST_KEY1_TIME, TEST_KEY2);
        JSONAssert.assertEquals(expected, result, false);
        final Map<String, Object> attributes = Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        histogram.put("attributes", attributes);
        final String resultAttr = histogram.toJsonString();
        assertThat(resultAttr.indexOf("attributes"), equalTo(-1));
    }

    @Test
    public void testHistogramToJsonStringWithAttributes() throws JSONException {
        histogram = builder.build(false);
        histogram.put("foo", "bar");
        final String value = UUID.randomUUID().toString();
        histogram.put("testObject", new TestObject(value));
        histogram.put("list", Arrays.asList(1, 4, 5));
        final Map<String, Object> attributes = Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        histogram.put("attributes", attributes);
        final String resultAttr = histogram.toJsonString();
        assertThat(resultAttr.indexOf("attributes"), not(equalTo(-1)));
    }
}
