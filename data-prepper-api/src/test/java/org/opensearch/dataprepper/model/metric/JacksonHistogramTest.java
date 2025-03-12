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
import org.opensearch.dataprepper.model.event.DefaultEventHandle;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.AggregateEventHandle;
import org.skyscreamer.jsonassert.JSONAssert;

import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JacksonHistogramTest {

    protected static final Long TEST_KEY1_TIME = new Date().getTime();
    protected static final String TEST_KEY2 = UUID.randomUUID().toString();
    protected static final Map<String, Object> TEST_ATTRIBUTES = ImmutableMap.of(
            "key1", TEST_KEY1_TIME,
            "key2", TEST_KEY2);
    protected static final String TEST_SERVICE_NAME = "service";
    protected static final String TEST_NAME = "name";
    protected static final String TEST_DESCRIPTION = "description";
    protected static final String TEST_UNIT_NAME = "unit";
    protected static final String TEST_START_TIME = UUID.randomUUID().toString();
    protected static final String TEST_TIME = UUID.randomUUID().toString();
    protected static final String TEST_EVENT_KIND = Metric.KIND.HISTOGRAM.name();
    protected static final Double TEST_SUM = 1D;
    protected static final Double TEST_MIN = 0.5D;
    protected static final Double TEST_MAX = 50.5D;
    protected static final List<Bucket> TEST_BUCKETS = Arrays.asList(
            new DefaultBucket(0.0, 5.0, 2L),
            new DefaultBucket(5.0, 10.0, 5L)
    );

    protected static final List<Long> TEST_BUCKET_COUNTS_LIST = Arrays.asList(1L, 2L, 3L);
    protected static final List<Double> TEST_EXPLICIT_BOUNDS_LIST = Arrays.asList(5D, 10D, 100D);
    protected static final Integer TEST_BUCKETS_COUNT = 2;
    protected static final Long TEST_COUNT = 2L;
    protected static final Integer TEST_EXPLICIT_BOUNDS_COUNT = 2;
    protected static final String TEST_AGGREGATION_TEMPORALITY = "AGGREGATIONTEMPORALITY";
    protected static final String TEST_SCHEMA_URL = "schema";

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
                .withMin(TEST_MIN)
                .withMax(TEST_MAX)
                .withCount(TEST_COUNT)
                .withBucketCount(TEST_BUCKETS_COUNT)
                .withBuckets(TEST_BUCKETS)
                .withExplicitBoundsCount(TEST_EXPLICIT_BOUNDS_COUNT)
                .withAggregationTemporality(TEST_AGGREGATION_TEMPORALITY)
                .withSchemaUrl(TEST_SCHEMA_URL)
                .withExplicitBoundsList(TEST_EXPLICIT_BOUNDS_LIST)
                .withBucketCountsList(TEST_BUCKET_COUNTS_LIST);

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
    public void testGetDefaultEventHandle() {
        EventHandle eventHandle = new DefaultEventHandle(Instant.now());
        builder.withEventHandle(eventHandle);
        histogram = builder.build();
        final EventHandle handle = histogram.getEventHandle();
        assertThat(handle, is(sameInstance(eventHandle)));
    }

    @Test
    public void testGetAggregateEventHandle() {
        EventHandle eventHandle = new AggregateEventHandle(Instant.now());
        builder.withEventHandle(eventHandle);
        histogram = builder.build();
        final EventHandle handle = histogram.getEventHandle();
        assertThat(handle, is(sameInstance(eventHandle)));
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
    public void testGetMin() {
        final Double min = histogram.getMin();
        assertThat(min, is(equalTo(TEST_MIN)));
    }

    @Test
    public void testGetMax() {
        final Double max = histogram.getMax();
        assertThat(max, is(equalTo(TEST_MAX)));
    }

    @Test
    public void testGetCount() {
        final Long count = histogram.getCount();
        assertThat(count, is(equalTo(TEST_COUNT)));
    }

    @Test
    public void testGetTimeReceived() {
        Instant now = Instant.now();
        builder.withTimeReceived(now);
        histogram = builder.build();
        assertThat(((DefaultEventHandle)histogram.getEventHandle()).getInternalOriginationTime(), is(now));
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
    public void testGetBucketCountsList() {
        List<Long> counts = histogram.getBucketCountsList();
        assertEquals(counts, TEST_BUCKET_COUNTS_LIST);
    }

    @Test
    public void testGetExplicitBoundsList() {
        List<Double> bounds = histogram.getExplicitBoundsList();
        assertEquals(bounds, TEST_EXPLICIT_BOUNDS_LIST);
    }

    @Test
    public void testGetBucketCounts() {
        Integer count = histogram.getBucketCount();
        assertEquals(count, TEST_BUCKETS_COUNT);
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
        assertThat(histogram.getAttributes(), is(anEmptyMap()));
    }

    @Test
    public void testHistogramToJsonString() throws Exception {
        histogram.put("foo", "bar");
        final String value = UUID.randomUUID().toString();
        histogram.put("testObject", new TestObject(value));
        histogram.put("list", Arrays.asList(1, 4, 5));
        final String result = histogram.toJsonString();

        String file = IOUtils.toString(this.getClass().getResourceAsStream("/testjson/histogram.json"));
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
