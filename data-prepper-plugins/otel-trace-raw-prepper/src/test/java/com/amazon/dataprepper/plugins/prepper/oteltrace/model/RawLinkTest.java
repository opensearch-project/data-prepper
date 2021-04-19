/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.prepper.oteltrace.model;

import com.google.protobuf.ByteString;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.trace.v1.Span;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.junit.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class RawLinkTest {
    @Test
    public void testRawLinkEmpty() {
        final Span.Link spanLinkEmpty = Span.Link.newBuilder().build();
        final RawLink rawLink = RawLink.buildRawLink(spanLinkEmpty);
        assertThat(rawLink.getAttributes().isEmpty()).isTrue();
        assertThat(rawLink.getSpanId()).isEmpty();
        assertThat(rawLink.getTraceId()).isEmpty();
        assertThat(rawLink.getTraceState()).isEmpty();
        assertThat(rawLink.getDroppedAttributesCount() == 0).isTrue();
    }

    @Test
    public void testRawEventWithAttributes() throws DecoderException {
        final KeyValue childAttr1 = KeyValue.newBuilder().setKey("statement").setValue(AnyValue.newBuilder()
                .setIntValue(1_000).build()).build();
        final KeyValue childAttr2 = KeyValue.newBuilder().setKey("statement.params").setValue(AnyValue.newBuilder()
                .setStringValue("us-east-1").build()).build();
        final Span.Link spanLink = Span.Link.newBuilder()
                .setSpanId(ByteString.copyFrom(TestUtils.getRandomBytes(8)))
                .setTraceId(ByteString.copyFrom(TestUtils.getRandomBytes(16)))
                .setTraceState("Some State")
                .addAllAttributes(Arrays.asList(childAttr1, childAttr2)).build();
        final RawLink rawLink = RawLink.buildRawLink(spanLink);
        assertThat(rawLink.getAttributes().size() == 2).isTrue();
        assertThat((Long) rawLink.getAttributes().get(childAttr1.getKey()) == childAttr1.getValue().getIntValue()).isTrue();
        assertThat(rawLink.getAttributes().get(OTelProtoHelper.REPLACE_DOT_WITH_AT.apply(childAttr2.getKey())).equals(childAttr2.getValue().getStringValue())).isTrue();
        assertThat(rawLink.getTraceState().equals("Some State")).isTrue();
        assertThat(ByteString.copyFrom(Hex.decodeHex(rawLink.getTraceId())).equals(spanLink.getTraceId())).isTrue();
        assertThat(ByteString.copyFrom(Hex.decodeHex(rawLink.getSpanId())).equals(spanLink.getSpanId())).isTrue();
    }
}
