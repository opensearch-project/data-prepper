/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.trace;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.TestObject;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DefaultLinkTest {

    private static final String TEST_SPAN_ID = UUID.randomUUID().toString();
    private static final String TEST_TRACE_ID = UUID.randomUUID().toString();
    private static final String TEST_TRACE_STATE = UUID.randomUUID().toString();
    private static final Map<String, Object> TEST_ATTRIBUTES = ImmutableMap.of("key1", UUID.randomUUID(), "key2", UUID.randomUUID().toString());
    private static final Integer TEST_DROPPED_ATTRIBUTE_COUNT = 8;

    private DefaultLink.Builder builder;

    private DefaultLink defaultLink;

    @BeforeEach
    public void setup() {

        builder = DefaultLink.builder()
                .withAttributes(TEST_ATTRIBUTES)
                .withDroppedAttributesCount(TEST_DROPPED_ATTRIBUTE_COUNT)
                .withSpanId(TEST_SPAN_ID)
                .withTraceId(TEST_TRACE_ID)
                .withTraceState(TEST_TRACE_STATE);

        defaultLink = builder.build();
    }

    @Test
    public void testGetSpanId() {
        final String spanId = defaultLink.getSpanId();
        assertThat(spanId, is(equalTo(TEST_SPAN_ID)));
    }

    @Test
    public void testGetTraceId() {
        final String traceId = defaultLink.getTraceId();
        assertThat(traceId, is(equalTo(TEST_TRACE_ID)));
    }

    @Test
    public void testGetTraceState() {
        final String traceState = defaultLink.getTraceState();
        assertThat(traceState, is(equalTo(TEST_TRACE_STATE)));
    }

    @Test
    public void testGetAttributes() {
        final Map<String,Object> attributes = defaultLink.getAttributes();
        assertThat(attributes, is(equalTo(TEST_ATTRIBUTES)));
    }

    @Test
    public void testGetDroppedAttributesCount() {
        final Integer droppedAttributesCount = defaultLink.getDroppedAttributesCount();
        assertThat(droppedAttributesCount, is(equalTo(TEST_DROPPED_ATTRIBUTE_COUNT)));
    }

    @Test
    public void testEquals_withDifferentObject() {
        assertThat(defaultLink, is(not(equalTo(new TestObject()))));
    }

    @Test
    public void testEquals_withDifferentSpanId() {
        final DefaultLink spanEvent = builder
                .withSpanId(UUID.randomUUID().toString())
                .build();
        assertThat(defaultLink, is(not(equalTo(spanEvent))));
    }

    @Test
    public void testEquals_withDifferentTraceId() {
        final DefaultLink spanEvent = builder
                .withTraceId(UUID.randomUUID().toString())
                .build();
        assertThat(defaultLink, is(not(equalTo(spanEvent))));
    }

    @Test
    public void testEquals_withDifferentTraceState() {
        final DefaultLink spanEvent = builder
                .withTraceState(UUID.randomUUID().toString())
                .build();
        assertThat(defaultLink, is(not(equalTo(spanEvent))));
    }

    @Test
    public void testEquals_withDifferentAttributes() {
        final DefaultLink spanEvent = builder
                .withAttributes(null)
                .build();
        assertThat(defaultLink, is(not(equalTo(spanEvent))));
    }

    @Test
    public void testEquals_withDifferentDroppedAttributesCount() {
        final DefaultLink spanEvent = builder
                .withDroppedAttributesCount(29034)
                .build();
        assertThat(defaultLink, is(not(equalTo(spanEvent))));
    }

    @Test
    public void testEquals() {
        final DefaultLink spanEvent = DefaultLink.builder()
                .withAttributes(TEST_ATTRIBUTES)
                .withDroppedAttributesCount(TEST_DROPPED_ATTRIBUTE_COUNT)
                .withSpanId(TEST_SPAN_ID)
                .withTraceId(TEST_TRACE_ID)
                .withTraceState(TEST_TRACE_STATE)
                .build();
        assertThat(defaultLink, is(equalTo(spanEvent)));
    }

    @Test
    public void testBuilder_withAllParameters_createsLink() {

        final DefaultLink defaultLink = DefaultLink.builder()
                .withAttributes(TEST_ATTRIBUTES)
                .withDroppedAttributesCount(TEST_DROPPED_ATTRIBUTE_COUNT)
                .withSpanId(TEST_SPAN_ID)
                .withTraceId(TEST_TRACE_ID)
                .withTraceState(TEST_TRACE_STATE)
                .build();

        assertThat(defaultLink, is(notNullValue()));
    }

    @Test
    public void testBuilder_withoutParameters_throwsNullPointerException() {
        final DefaultLink.Builder builder = DefaultLink.builder();
        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    public void testBuilder_withMissingSpanId_throwsNullPointerException() {

        builder.withSpanId(null);

        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    public void testBuilder_withMissingTraceId_throwsNullPointerException() {

        builder.withTraceId(null);

        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    public void testBuilder_withEmptySpanId_throwsIllegalArgumentException() {

        builder.withSpanId("");

        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    public void testBuilder_withEmptyTraceId_throwsIllegalArgumentException() {

        builder.withTraceId("");

        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    public void testBuilder_withMissingAttributes_setsAttributesToEmptySet() {

        final DefaultLink result = builder.withAttributes(null)
                .build();

        assertThat(result.getAttributes(), is(equalTo(new HashMap<>())));
    }

    @Test
    public void testBuilder_withMissingDroppedAttributesCount_setsCountToZero() {

        final DefaultLink result = builder.withDroppedAttributesCount(null)
                .build();

        assertThat(result.getDroppedAttributesCount(), is(equalTo(0)));
    }
}
