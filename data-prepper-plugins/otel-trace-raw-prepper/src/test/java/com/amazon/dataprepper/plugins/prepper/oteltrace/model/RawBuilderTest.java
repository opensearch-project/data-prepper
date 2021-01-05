package com.amazon.dataprepper.plugins.prepper.oteltrace.model;

import com.google.protobuf.ByteString;
import io.opentelemetry.proto.common.v1.InstrumentationLibrary;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.Status;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.junit.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

public class RawBuilderTest {
    @Test
    public void testRawSpan() throws DecoderException {
        final Span.Event event1 = Span.Event.newBuilder().setName("event-1").setTimeUnixNano(651242400000000321L + 1000).build();
        final Span.Event event2 = Span.Event.newBuilder().setName("event-2").setTimeUnixNano(651242400000000321L + 2000).setDroppedAttributesCount(0).build();
        final Span.Link link1 = Span.Link.newBuilder().setTraceId(ByteString.copyFrom(TestUtils.getRandomBytes(16)))
                .setSpanId(ByteString.copyFrom(TestUtils.getRandomBytes(8)))
                .build();
        final Span span = Span.newBuilder()
                .setTraceId(ByteString.copyFrom(TestUtils.getRandomBytes(16)))
                .setSpanId(ByteString.copyFrom(TestUtils.getRandomBytes(8)))
                .setParentSpanId(ByteString.copyFrom(TestUtils.getRandomBytes(8)))
                .setTraceState("some state")
                .setName("test-span")
                .setKind(Span.SpanKind.SPAN_KIND_CONSUMER)
                .setStartTimeUnixNano(651242400000000321L)
                .setEndTimeUnixNano(651242400000000321L + 3000)
                .setStatus(Status.newBuilder().setCodeValue(Status.StatusCode.STATUS_CODE_UNSET_VALUE).setMessage("status-description").build())
                .addAttributes(KeyValue.newBuilder()
                        .setKey("some-key")
                        .build()
                )
                .setDroppedAttributesCount(1)
                .addEvents(event1)
                .addEvents(event2)
                .addLinks(link1)
                .build();
        final RawSpan rawSpan = new RawSpanBuilder().setFromSpan(span, InstrumentationLibrary.newBuilder().build(), "some-service", Collections.EMPTY_MAP).build();
        assertThat(ByteString.copyFrom(Hex.decodeHex(rawSpan.getTraceId())).equals(span.getTraceId())).isTrue();
        assertThat(ByteString.copyFrom(Hex.decodeHex(rawSpan.getSpanId())).equals(span.getSpanId())).isTrue();
        assertThat(ByteString.copyFrom(Hex.decodeHex(rawSpan.getParentSpanId())).equals(span.getParentSpanId())).isTrue();
        assertThat(rawSpan.getTraceState().equals(span.getTraceState())).isTrue();
        assertThat(rawSpan.getName().equals(span.getName())).isTrue();
        assertThat(rawSpan.getKind().equals(span.getKind().name())).isTrue();
        assertThat(rawSpan.getDurationInNanos()).isEqualTo(3000);
        assertThat(rawSpan.getStartTime().equals(OTelProtoHelper.getStartTimeISO8601(span))).isTrue();
        assertThat(rawSpan.getEndTime().equals(OTelProtoHelper.getEndTimeISO8601(span))).isTrue();
        assertThat(rawSpan.getAttributes().get(OTelProtoHelper.STATUS_MESSAGE)).isEqualTo("status-description");
        assertThat(rawSpan.getAttributes().get(OTelProtoHelper.STATUS_CODE)).isEqualTo(0);
        assertThat(rawSpan.getAttributes().get(OTelProtoHelper.SPAN_ATTRIBUTES_REPLACE_DOT_WITH_AT.apply("some-key")).equals("")).isTrue();
        assertThat(rawSpan.getDroppedAttributesCount()).isEqualTo(1);
        assertThat(rawSpan.getDroppedLinksCount()).isEqualTo(0);
        assertThat(rawSpan.getDroppedEventsCount()).isEqualTo(0);
        assertThat(rawSpan.getEvents().size()).isEqualTo(2);
        assertThat(rawSpan.getLinks().size()).isEqualTo(1);
        assertThat(rawSpan.getServiceName()).isEqualTo("some-service");
        assertThat(rawSpan.getTraceGroup()).isNull();
    }

    @Test
    public void testForTraceGroupRawSpan() throws DecoderException {
        final Span span = Span.newBuilder()
                .setTraceId(ByteString.copyFrom(TestUtils.getRandomBytes(16)))
                .setSpanId(ByteString.copyFrom(TestUtils.getRandomBytes(8)))
                .setName("test-span")
                .build();
        final RawSpan rawSpan = new RawSpanBuilder().setFromSpan(span, InstrumentationLibrary.newBuilder().build(), "some-service", Collections.EMPTY_MAP).build();
        assertThat(ByteString.copyFrom(Hex.decodeHex(rawSpan.getTraceId())).equals(span.getTraceId())).isTrue();
        assertThat(ByteString.copyFrom(Hex.decodeHex(rawSpan.getSpanId())).equals(span.getSpanId())).isTrue();
        assertThat(rawSpan.getTraceGroup()).isEqualTo(rawSpan.getName());
    }

    /**
     * You can submit empty object and the prepper will process it without any error
     */
    @Test
    public void testRawSpanEmpty() {
        final RawSpan rawSpan = new RawSpanBuilder().setFromSpan(Span.newBuilder().build(), InstrumentationLibrary.newBuilder().build(), null, Collections.EMPTY_MAP).build();
        assertThat(rawSpan.getSpanId()).isEmpty();
    }
}
