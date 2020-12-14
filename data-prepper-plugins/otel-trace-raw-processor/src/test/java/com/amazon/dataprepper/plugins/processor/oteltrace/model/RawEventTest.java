package com.amazon.dataprepper.plugins.processor.oteltrace.model;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.trace.v1.Span;
import org.junit.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class RawEventTest {


    @Test
    public void testRawEventEmpty() {
        final Span.Event spanEventEmpty = Span.Event.newBuilder().build();
        final RawEvent rawEventWithEmptyAttri = RawEvent.buildRawEvent(spanEventEmpty);
        assertThat(rawEventWithEmptyAttri.getAttributes().isEmpty()).isTrue();
        assertThat(rawEventWithEmptyAttri.getName()).isEmpty();
        assertThat(rawEventWithEmptyAttri.getTime().equals("1970-01-01T00:00:00Z")).isTrue();
        assertThat(rawEventWithEmptyAttri.getDroppedAttributesCount() == 0).isTrue();

    }

    @Test
    public void testRawEvent() {
        final Span.Event spanEventWithEmptyAttri = Span.Event.newBuilder().setName("Error").setTimeUnixNano(1598013600000000321L).build();
        final RawEvent rawEventWithEmptyAttri = RawEvent.buildRawEvent(spanEventWithEmptyAttri);
        assertThat(rawEventWithEmptyAttri.getAttributes().isEmpty()).isTrue();
        assertThat(rawEventWithEmptyAttri.getName().equals("Error")).isTrue();
        assertThat(rawEventWithEmptyAttri.getTime().equals("2020-08-21T12:40:00.000000321Z")).isTrue();
    }

    @Test
    public void testRawEventWithAttributes() {
        final KeyValue childAttr1 = KeyValue.newBuilder().setKey("statement").setValue(AnyValue.newBuilder()
                .setIntValue(1_000).build()).build();
        final KeyValue childAttr2 = KeyValue.newBuilder().setKey("statement.params").setValue(AnyValue.newBuilder()
                .setStringValue("us-east-1").build()).build();
        final Span.Event spanEvent = Span.Event.newBuilder().setName("Error").setTimeUnixNano(1598013600000000321L)
                .addAllAttributes(Arrays.asList(childAttr1, childAttr2)).build();
        final RawEvent rawEvent = RawEvent.buildRawEvent(spanEvent);
        assertThat(rawEvent.getAttributes().size() == 2).isTrue();
        assertThat((Long) rawEvent.getAttributes().get(childAttr1.getKey()) == childAttr1.getValue().getIntValue()).isTrue();
        assertThat(rawEvent.getAttributes().get(OTelProtoHelper.REPLACE_DOT_WITH_AT.apply(childAttr2.getKey())).equals(childAttr2.getValue().getStringValue())).isTrue();
        assertThat(rawEvent.getName().equals("Error")).isTrue();
        assertThat(rawEvent.getTime().equals("2020-08-21T12:40:00.000000321Z")).isTrue();
    }
}
