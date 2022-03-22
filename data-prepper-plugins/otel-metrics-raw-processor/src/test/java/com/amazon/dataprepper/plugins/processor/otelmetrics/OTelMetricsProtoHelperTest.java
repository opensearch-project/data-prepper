/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.otelmetrics;

import com.google.protobuf.ByteString;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.ArrayValue;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import org.junit.jupiter.api.Test;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.*;

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
    public void testCreateBuckets() {
        assertThat(OTelMetricsProtoHelper.createBuckets(new ArrayList<>(),new ArrayList<>()).size(),equalTo(0));
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