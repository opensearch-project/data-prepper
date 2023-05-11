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

public class DefaultSpanEventTest {

    private static final String TEST_NAME = UUID.randomUUID().toString();
    private static final String TEST_TIME = UUID.randomUUID().toString();
    private static final Map<String, Object> TEST_ATTRIBUTES = ImmutableMap.of("key1", UUID.randomUUID(), "key2", UUID.randomUUID().toString());
    private static final Integer TEST_DROPPED_ATTRIBUTE_COUNT = 3;

    private DefaultSpanEvent defaultSpanEvent;

    private DefaultSpanEvent.Builder builder;

    @BeforeEach
    public void setup() {

        builder = DefaultSpanEvent.builder()
                .withName(TEST_NAME)
                .withTime(TEST_TIME)
                .withAttributes(TEST_ATTRIBUTES)
                .withDroppedAttributesCount(TEST_DROPPED_ATTRIBUTE_COUNT);

        defaultSpanEvent = builder.build();
    }

    @Test
    public void testGetName() {
        final String name = defaultSpanEvent.getName();
        assertThat(name, is(equalTo(TEST_NAME)));
    }

    @Test
    public void testGetTime() {
        final String time = defaultSpanEvent.getTime();
        assertThat(time, is(equalTo(TEST_TIME)));
    }


    @Test
    public void testGetAttributes() {
        final Map<String,Object> attributes = defaultSpanEvent.getAttributes();
        assertThat(attributes, is(equalTo(TEST_ATTRIBUTES)));
    }

    @Test
    public void testGetDroppedAttributesCount() {
        final Integer droppedAttributesCount = defaultSpanEvent.getDroppedAttributesCount();
        assertThat(droppedAttributesCount, is(equalTo(TEST_DROPPED_ATTRIBUTE_COUNT)));
    }

    @Test
    public void testEquals_withDifferentObject() {
        assertThat(defaultSpanEvent, is(not(equalTo(new TestObject()))));
    }

    @Test
    public void testEquals_withDifferentTime() {
        final DefaultSpanEvent spanEvent = builder
                .withTime(UUID.randomUUID().toString())
                .build();
        assertThat(defaultSpanEvent, is(not(equalTo(spanEvent))));
    }

    @Test
    public void testEquals_withDifferentName() {
        final DefaultSpanEvent spanEvent = builder
                .withName(UUID.randomUUID().toString())
                .build();
        assertThat(defaultSpanEvent, is(not(equalTo(spanEvent))));
    }

    @Test
    public void testEquals_withDifferentAttributes() {
        final DefaultSpanEvent spanEvent = builder
                .withAttributes(null)
                .build();
        assertThat(defaultSpanEvent, is(not(equalTo(spanEvent))));
    }

    @Test
    public void testEquals_withDifferentDroppedAttributesCount() {
        final DefaultSpanEvent spanEvent = builder
                .withDroppedAttributesCount(29034)
                .build();
        assertThat(defaultSpanEvent, is(not(equalTo(spanEvent))));
    }

    @Test
    public void testEquals() {
        final DefaultSpanEvent spanEvent = DefaultSpanEvent.builder()
                .withName(TEST_NAME)
                .withTime(TEST_TIME)
                .withAttributes(TEST_ATTRIBUTES)
                .withDroppedAttributesCount(TEST_DROPPED_ATTRIBUTE_COUNT)
                .build();
        assertThat(defaultSpanEvent, is(equalTo(spanEvent)));
    }

    @Test
    public void testBuilder_withValidParameters_createsEvent() {
        final DefaultSpanEvent spanEvent = DefaultSpanEvent.builder()
                .withName(TEST_NAME)
                .withTime(TEST_TIME)
                .withAttributes(TEST_ATTRIBUTES)
                .withDroppedAttributesCount(TEST_DROPPED_ATTRIBUTE_COUNT)
                .build();
        assertThat(spanEvent, is(notNullValue()));
    }

    @Test
    public void testBuilder_withoutParameters_throwsNullPointerException() {
        final DefaultSpanEvent.Builder builder = DefaultSpanEvent.builder();
        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    public void testBuilder_withMissingName_throwsNullPointerException() {
        builder.withName(null);
        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    public void testBuilder_withEmptyName_throwsIllegalArgumentException() {
        builder.withName("");
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    public void testBuilder_withMissingTime_throwsNullPointerException() {
        builder.withTime(null);
        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    public void testBuilder_withEmptyTime_throwsIllegalArgumentException() {
        builder.withTime("");
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    public void testBuilder_withMissingAttributes_setsAttributesToEmptySet() {

        final DefaultSpanEvent result = builder.withAttributes(null)
                .build();
        assertThat(result.getAttributes(), is(equalTo(new HashMap<>())));
    }

    @Test
    public void testBuilder_withMissingDroppedAttributesCount_setsCountToZero() {
        final DefaultSpanEvent result = builder.withDroppedAttributesCount(null)
                .build();
        assertThat(result.getDroppedAttributesCount(), is(equalTo(0)));
    }
}
