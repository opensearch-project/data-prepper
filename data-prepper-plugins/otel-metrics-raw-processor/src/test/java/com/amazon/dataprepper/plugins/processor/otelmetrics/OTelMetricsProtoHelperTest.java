/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.otelmetrics;

import com.amazon.dataprepper.model.metric.Bucket;
import com.google.protobuf.ByteString;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * This test exists purely to satisfy the test coverage because OtelMetricsHelper must be merged with
 * OtelProtoCodec when #546 is integrated since it shares most of the code with OTelProtoCodec
  */
public class OTelMetricsProtoHelperTest {

    @Test
    void getValueAsDouble() {
        assertNull(OTelMetricsProtoHelper.getValueAsDouble(NumberDataPoint.newBuilder().build()));
    }

    @Test
    public void testCreateBucketsEmpty() {
        assertThat(OTelMetricsProtoHelper.createBuckets(new ArrayList<>(),new ArrayList<>()).size(),equalTo(0));
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
}