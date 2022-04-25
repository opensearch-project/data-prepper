/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.otelmetrics;

import com.google.protobuf.ByteString;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.ArrayValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.Exemplar;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import org.apache.commons.codec.binary.Hex;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.metric.Bucket;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.entry;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * This test exists purely to satisfy the test coverage because OtelMetricsHelper must be merged with
 * OtelProtoCodec when #546 is integrated since it shares most of the code with OTelProtoCodec
 */
public class OTelMetricsProtoHelperTest {

    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochSecond(1_700_000_000), ZoneOffset.UTC);

    private static final Double MAX_ERROR = 0.00001;
    private static final Random RANDOM = new Random();

    public static byte[] getRandomBytes(int len) {
        byte[] bytes = new byte[len];
        RANDOM.nextBytes(bytes);
        return bytes;
    }

    @Test
    void getValueAsDouble() {
        assertNull(OTelMetricsProtoHelper.getValueAsDouble(NumberDataPoint.newBuilder().build()));
    }

    @Test
    public void testCreateBucketsEmpty() {
        assertThat(OTelMetricsProtoHelper.createBuckets(new ArrayList<>(), new ArrayList<>()).size(), equalTo(0));
    }

    @Test
    public void testCreateBuckets() {
        List<Long> bucketsCountList = Arrays.asList(1L, 2L, 3L, 4L);
        List<Double> explicitBOundsList = Arrays.asList(5D, 10D, 25D);
        List<Bucket> buckets = OTelMetricsProtoHelper.createBuckets(bucketsCountList, explicitBOundsList);
        assertThat(buckets.size(), equalTo(4));
        Bucket b1 = buckets.get(0);
        assertThat(b1.getCount(), equalTo(1L));
        assertThat(b1.getMin(), equalTo((double) -Float.MAX_VALUE));
        assertThat(b1.getMax(), equalTo(5D));

        Bucket b2 = buckets.get(1);
        assertThat(b2.getCount(), equalTo(2L));
        assertThat(b2.getMin(), equalTo(5D));
        assertThat(b2.getMax(), equalTo(10D));

        Bucket b3 = buckets.get(2);
        assertThat(b3.getCount(), equalTo(3L));
        assertThat(b3.getMin(), equalTo(10D));
        assertThat(b3.getMax(), equalTo(25D));

        Bucket b4 = buckets.get(3);
        assertThat(b4.getCount(), equalTo(4L));
        assertThat(b4.getMin(), equalTo(25D));
        assertThat(b4.getMax(), equalTo((double) Float.MAX_VALUE));
    }

    @Test
    public void testCreateBuckets_illegal_argument() {
        List<Long> bucketsCountList = Arrays.asList(1L, 2L, 3L, 4L);
        List<Double> boundsList = Collections.emptyList();
        assertThrows(IllegalArgumentException.class, () -> OTelMetricsProtoHelper.createBuckets(bucketsCountList, boundsList));
    }


    @Test
    public void testConvertAnyValueBool() {
        Object o = OTelMetricsProtoHelper.convertAnyValue(AnyValue.newBuilder().setBoolValue(true).build());
        assertThat(o instanceof Boolean, equalTo(true));
        assertThat(((boolean) o), equalTo(true));
    }

    @Test
    public void testUnsupportedTypeToAnyValue() {
        assertThrows(RuntimeException.class,
                () -> OTelMetricsProtoHelper.convertAnyValue(AnyValue.newBuilder().setBytesValue(ByteString.EMPTY).build()));
    }

    @Test
    void convertExemplars() {
        long t1 = TimeUnit.MILLISECONDS.toNanos(Instant.now(CLOCK).toEpochMilli());
        long t2 = t1 + 100_000;

        Exemplar e1 = Exemplar.newBuilder()
                .addFilteredAttributes(KeyValue.newBuilder()
                        .setKey("key")
                        .setValue(AnyValue.newBuilder().setBoolValue(true).build()).build())
                .setAsDouble(3)
                .setSpanId(ByteString.copyFrom(getRandomBytes(8)))
                .setTimeUnixNano(t1)
                .setTraceId(ByteString.copyFrom(getRandomBytes(8)))
                .build();


        Exemplar e2 = Exemplar.newBuilder()
                .addFilteredAttributes(KeyValue.newBuilder()
                        .setKey("key2")
                        .setValue(AnyValue.newBuilder()
                                .setArrayValue(ArrayValue.newBuilder().addValues(AnyValue.newBuilder().setStringValue("test").build()).build())
                                .build()).build())
                .setAsInt(42)
                .setSpanId(ByteString.copyFrom(getRandomBytes(8)))
                .setTimeUnixNano(t2)
                .setTraceId(ByteString.copyFrom(getRandomBytes(8)))
                .build();

        List<io.opentelemetry.proto.metrics.v1.Exemplar> exemplars = Arrays.asList(e1, e2);
        List<org.opensearch.dataprepper.model.metric.Exemplar> convertedExemplars = OTelMetricsProtoHelper.convertExemplars(exemplars);
        assertThat(convertedExemplars.size(), equalTo(2));

        org.opensearch.dataprepper.model.metric.Exemplar conv1 = convertedExemplars.get(0);
        assertThat(conv1.getSpanId(), equalTo(Hex.encodeHexString(e1.getSpanId().toByteArray())));
        assertThat(conv1.getTime(), equalTo("2023-11-14T22:13:20Z"));
        assertThat(conv1.getTraceId(), equalTo(Hex.encodeHexString(e1.getTraceId().toByteArray())));
        assertThat(conv1.getValue(), equalTo(3.0));
        Assertions.assertThat(conv1.getAttributes()).contains(entry("exemplar.attributes.key", true));

        org.opensearch.dataprepper.model.metric.Exemplar conv2 = convertedExemplars.get(1);
        assertThat(conv2.getSpanId(), equalTo(Hex.encodeHexString(e2.getSpanId().toByteArray())));
        assertThat(conv2.getTime(), equalTo("2023-11-14T22:13:20.000100Z"));
        assertThat(conv2.getTraceId(), equalTo(Hex.encodeHexString(e2.getTraceId().toByteArray())));
        assertThat(conv2.getValue(), equalTo(42.0));
        Assertions.assertThat(conv2.getAttributes()).contains(entry("exemplar.attributes.key2", "[\"test\"]"));

    }


    /**
     * See: <a href="https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/datamodel.md#exponential-buckets">The example table with scale 3</a>
     */
    @Test
    public void testExponentialHistogram() {
        List<Bucket> b = OTelMetricsProtoHelper.createExponentialBuckets(
                ExponentialHistogramDataPoint.Buckets.newBuilder()
                        .addBucketCounts(4)
                        .addBucketCounts(2)
                        .addBucketCounts(3)
                        .addBucketCounts(2)
                        .addBucketCounts(1)
                        .addBucketCounts(4)
                        .addBucketCounts(6)
                        .addBucketCounts(4)
                        .setOffset(0)
                        .build(), 3);

        assertThat(b.size(), equalTo(8));

        Bucket b1 = b.get(0);
        assertThat(b1.getCount(), equalTo(4L));
        assertThat(b1.getMin(), equalTo(1D));
        assertThat(b1.getMax(), closeTo(1.09051, MAX_ERROR));

        Bucket b2 = b.get(1);
        assertThat(b2.getCount(), equalTo(2L));
        assertThat(b2.getMin(), closeTo(1.09051, MAX_ERROR));
        assertThat(b2.getMax(), closeTo(1.18921, MAX_ERROR));

        Bucket b3 = b.get(2);
        assertThat(b3.getCount(), equalTo(3L));
        assertThat(b3.getMin(), closeTo(1.18921, MAX_ERROR));
        assertThat(b3.getMax(), closeTo(1.29684, MAX_ERROR));

        Bucket b4 = b.get(3);
        assertThat(b4.getCount(), equalTo(2L));
        assertThat(b4.getMin(), closeTo(1.29684, MAX_ERROR));
        assertThat(b4.getMax(), closeTo(1.41421, MAX_ERROR));

        Bucket b5 = b.get(4);
        assertThat(b5.getCount(), equalTo(1L));
        assertThat(b5.getMin(), closeTo(1.41421, MAX_ERROR));
        assertThat(b5.getMax(), closeTo(1.54221, MAX_ERROR));

        Bucket b6 = b.get(5);
        assertThat(b6.getCount(), equalTo(4L));
        assertThat(b6.getMin(), closeTo(1.54221, MAX_ERROR));
        assertThat(b6.getMax(), closeTo(1.68179, MAX_ERROR));

        Bucket b7 = b.get(6);
        assertThat(b7.getCount(), equalTo(6L));
        assertThat(b7.getMin(), closeTo(1.68179, MAX_ERROR));
        assertThat(b7.getMax(), closeTo(1.83401, MAX_ERROR));

        Bucket b8 = b.get(7);
        assertThat(b8.getCount(), equalTo(4L));
        assertThat(b8.getMin(), closeTo(1.83401, MAX_ERROR));
        assertThat(b8.getMax(), closeTo(2, MAX_ERROR));
    }

    @Test
    public void testExponentialHistogramWithOffset() {
        List<Bucket> b = OTelMetricsProtoHelper.createExponentialBuckets(
                ExponentialHistogramDataPoint.Buckets.newBuilder()
                        .addBucketCounts(4)
                        .addBucketCounts(2)
                        .addBucketCounts(3)
                        .addBucketCounts(2)
                        .addBucketCounts(1)
                        .addBucketCounts(4)
                        .setOffset(2)
                        .build(), 3);

        assertThat(b.size(), equalTo(6));

        Bucket b1 = b.get(0);
        assertThat(b1.getCount(), equalTo(4L));
        assertThat(b1.getMin(), closeTo(1.18920, MAX_ERROR));
        assertThat(b1.getMax(), closeTo(1.29684, MAX_ERROR));

        Bucket b2 = b.get(1);
        assertThat(b2.getCount(), equalTo(2L));
        assertThat(b2.getMin(), closeTo(1.29684, MAX_ERROR));
        assertThat(b2.getMax(), closeTo(1.41421, MAX_ERROR));

        Bucket b3 = b.get(2);
        assertThat(b3.getCount(), equalTo(3L));
        assertThat(b3.getMin(), closeTo(1.41421, MAX_ERROR));
        assertThat(b3.getMax(), closeTo(1.54221, MAX_ERROR));

        Bucket b4 = b.get(3);
        assertThat(b4.getCount(), equalTo(2L));
        assertThat(b4.getMin(), closeTo(1.54221, MAX_ERROR));
        assertThat(b4.getMax(), closeTo(1.68179, MAX_ERROR));

        Bucket b5 = b.get(4);
        assertThat(b5.getCount(), equalTo(1L));
        assertThat(b5.getMin(), closeTo(1.68179, MAX_ERROR));
        assertThat(b5.getMax(), closeTo(1.83401, MAX_ERROR));

        Bucket b6 = b.get(5);
        assertThat(b6.getCount(), equalTo(4L));
        assertThat(b6.getMin(), closeTo(1.83401, MAX_ERROR));
        assertThat(b6.getMax(), closeTo(2, MAX_ERROR));
    }

    @Test
    public void testExponentialHistogramWithNegativeScale() {
        List<Bucket> b = OTelMetricsProtoHelper.createExponentialBuckets(
                ExponentialHistogramDataPoint.Buckets.newBuilder()
                        .addBucketCounts(4)
                        .addBucketCounts(2)
                        .addBucketCounts(3)
                        .addBucketCounts(2)
                        .setOffset(0)
                        .build(), -3);

        assertThat(b.size(), equalTo(4));

        Bucket b1 = b.get(0);
        assertThat(b1.getCount(), equalTo(4L));
        assertThat(b1.getMin(), closeTo(2, MAX_ERROR));
        assertThat(b1.getMax(), closeTo(4, MAX_ERROR));

        Bucket b2 = b.get(1);
        assertThat(b2.getCount(), equalTo(2L));
        assertThat(b2.getMin(), closeTo(4, MAX_ERROR));
        assertThat(b2.getMax(), closeTo(16, MAX_ERROR));

        Bucket b3 = b.get(2);
        assertThat(b3.getCount(), equalTo(3L));
        assertThat(b3.getMin(), closeTo(16, MAX_ERROR));
        assertThat(b3.getMax(), closeTo(256, MAX_ERROR));

        Bucket b4 = b.get(3);
        assertThat(b4.getCount(), equalTo(2L));
        assertThat(b4.getMin(), closeTo(256, MAX_ERROR));
        assertThat(b4.getMax(), closeTo(65536, MAX_ERROR));
    }
}
