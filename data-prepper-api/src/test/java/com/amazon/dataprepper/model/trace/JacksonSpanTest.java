/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.model.trace;

import com.amazon.dataprepper.model.event.JacksonEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class JacksonSpanTest {
    private static final ObjectMapper mapper = new ObjectMapper();

    private static final String TEST_TRACE_ID =  UUID.randomUUID().toString();
    private static final String TEST_SPAN_ID =  UUID.randomUUID().toString();
    private static final String TEST_TRACE_STATE =  UUID.randomUUID().toString();
    private static final String TEST_PARENT_SPAN_ID =  UUID.randomUUID().toString();
    private static final String TEST_NAME =  UUID.randomUUID().toString();
    private static final String TEST_KIND =  UUID.randomUUID().toString();
    private static final String TEST_START_TIME =  UUID.randomUUID().toString();
    private static final String TEST_END_TIME =  UUID.randomUUID().toString();
    private static final Map<String, Object> TEST_ATTRIBUTES = ImmutableMap.of("key1", new Date().getTime(), "key2", UUID.randomUUID().toString());
    private static final Integer TEST_DROPPED_ATTRIBUTES_COUNT = 8;
    private static final Integer TEST_DROPPED_EVENTS_COUNT =  45;
    private static final Integer TEST_DROPPED_LINKS_COUNT =  21;
    private static final String TEST_TRACE_GROUP =  UUID.randomUUID().toString();
    private static final Long TEST_DURATION_IN_NANOS =  537L;
    private static final String TEST_SERVICE_NAME = UUID.randomUUID().toString();

    private JacksonSpan.Builder builder;
    
    private JacksonSpan jacksonSpan;

    private DefaultLink defaultLink;

    private DefaultSpanEvent defaultSpanEvent;

    private DefaultTraceGroupFields defaultTraceGroupFields;
    
    @BeforeEach
    public void setup() {

        // Elected to use actual data objects instead of mocking as mockito would require public setters or altering the underlying
        // object mapper in the JacksonEvent class to correctly serialize this objects.
        defaultSpanEvent = DefaultSpanEvent.builder()
                .withName(UUID.randomUUID().toString())
                .withTime(UUID.randomUUID().toString())
                .build();

        defaultLink = DefaultLink.builder()
                .withTraceId(UUID.randomUUID().toString())
                .withSpanId(UUID.randomUUID().toString())
                .withTraceState(UUID.randomUUID().toString())
                .build();

        defaultTraceGroupFields = DefaultTraceGroupFields.builder()
                .withDurationInNanos(123L)
                .withStatusCode(201)
                .withEndTime("the End")
                .build();
        
        builder = JacksonSpan.builder()
                .withSpanId(TEST_SPAN_ID)
                .withTraceId(TEST_TRACE_ID)
                .withTraceState(TEST_TRACE_STATE)
                .withParentSpanId(TEST_PARENT_SPAN_ID)
                .withName(TEST_NAME)
                .withServiceName(TEST_SERVICE_NAME)
                .withKind(TEST_KIND)
                .withStartTime(TEST_START_TIME)
                .withEndTime(TEST_END_TIME)
                .withAttributes(TEST_ATTRIBUTES)
                .withDroppedAttributesCount(TEST_DROPPED_ATTRIBUTES_COUNT)
                .withEvents(Arrays.asList(defaultSpanEvent))
                .withDroppedEventsCount(TEST_DROPPED_EVENTS_COUNT)
                .withLinks(Arrays.asList(defaultLink))
                .withDroppedLinksCount(TEST_DROPPED_LINKS_COUNT)
                .withTraceGroup(TEST_TRACE_GROUP)
                .withDurationInNanos(TEST_DURATION_IN_NANOS)
                .withTraceGroupFields(defaultTraceGroupFields);

        jacksonSpan = builder.build();
    }

    @Test
    public void testGetSpanId() {
        final String spanId = jacksonSpan.getSpanId();
        assertThat(spanId, is(equalTo(TEST_SPAN_ID)));
    }

    @Test
    public void testGetTraceId() {
        final String traceId = jacksonSpan.getTraceId();
        assertThat(traceId, is(equalTo(TEST_TRACE_ID)));
    }

    @Test
    public void testGetTraceState() {
        final String traceState = jacksonSpan.getTraceState();
        assertThat(traceState, is(equalTo(TEST_TRACE_STATE)));
    }

    @Test
    public void testGetParentSpanId() {
        final String parentSpanId = jacksonSpan.getParentSpanId();
        assertThat(parentSpanId, is(equalTo(TEST_PARENT_SPAN_ID)));
    }

    @Test
    public void testGetName() {
        final String name = jacksonSpan.getName();
        assertThat(name, is(equalTo(TEST_NAME)));
    }

    @Test
    public void testGetServiceName() {
        final String name = jacksonSpan.getServiceName();
        assertThat(name, is(equalTo(TEST_SERVICE_NAME)));
    }

    @Test
    public void testGetKind() {
        final String kind = jacksonSpan.getKind();
        assertThat(kind, is(equalTo(TEST_KIND)));
    }

    @Test
    public void testGetStartTime() {
        final String GetStartTime = jacksonSpan.getStartTime();
        assertThat(GetStartTime, is(equalTo(TEST_START_TIME)));
    }

    @Test
    public void testGetEndTime() {
        final String endTime = jacksonSpan.getEndTime();
        assertThat(endTime, is(equalTo(TEST_END_TIME)));
    }

    @Test
    public void testGetAttributes() {
        final Map<String,Object> attributes = jacksonSpan.getAttributes();

        TEST_ATTRIBUTES.keySet().forEach(key -> {
            assertThat(attributes, hasKey(key));
            assertThat(attributes.get(key), is(equalTo(TEST_ATTRIBUTES.get(key))));
                }
        );
    }

    @Test
    public void testGetDroppedAttributesCount() {
        final Integer droppedAttributesCount = jacksonSpan.getDroppedAttributesCount();
        assertThat(droppedAttributesCount, is(equalTo(TEST_DROPPED_ATTRIBUTES_COUNT)));
    }

    @Test
    public void testGetEvents() {
        final List events = jacksonSpan.getEvents();
        assertThat(events, is(equalTo(Arrays.asList(defaultSpanEvent))));
    }

    @Test
    public void testGetDroppedEventsCount() {
        final Integer droppedEventsCount = jacksonSpan.getDroppedEventsCount();
        assertThat(droppedEventsCount, is(equalTo(TEST_DROPPED_EVENTS_COUNT)));
    }

    @Test
    public void testGetLinks() {
        final List links = jacksonSpan.getLinks();
        assertThat(links, is(equalTo(Arrays.asList(defaultLink))));
    }

    @Test
    public void testGetDroppedLinksCount() {
        final Integer droppedLinksCount = jacksonSpan.getDroppedLinksCount();
        assertThat(droppedLinksCount, is(equalTo(TEST_DROPPED_LINKS_COUNT)));
    }

    @Test
    public void testGetTraceGroup() {
        final String traceGroup = jacksonSpan.getTraceGroup();
        assertThat(traceGroup, is(equalTo(TEST_TRACE_GROUP)));
    }

    @Test
    public void testGetDurationInNanos() {
        final Long durationInNanos = jacksonSpan.getDurationInNanos();

        assertThat(durationInNanos, is(TEST_DURATION_IN_NANOS));
    }

    @Test
    public void testGetTraceGroupFields() {
        final TraceGroupFields traceGroupFields = jacksonSpan.getTraceGroupFields();
        assertThat(traceGroupFields, is(equalTo(traceGroupFields)));
    }

    @Test
    public void testSetAndGetTraceGroup() {
        final String testTraceGroup = "testTraceGroup";
        jacksonSpan.setTraceGroup(testTraceGroup);
        final String traceGroup = jacksonSpan.getTraceGroup();
        assertThat(traceGroup, is(equalTo(testTraceGroup)));
    }

    @Test
    public void testSetAndGetTraceGroupFields() {
        final TraceGroupFields testTraceGroupFields = DefaultTraceGroupFields.builder()
                .withDurationInNanos(200L)
                .withStatusCode(404)
                .withEndTime("Different end time")
                .build();
        jacksonSpan.setTraceGroupFields(testTraceGroupFields);
        final TraceGroupFields traceGroupFields = jacksonSpan.getTraceGroupFields();
        assertThat(traceGroupFields, is(equalTo(traceGroupFields)));
        assertThat(traceGroupFields, is(equalTo(testTraceGroupFields)));
    }

    @Test
    public void testToJsonStringAllParameters() throws JsonProcessingException {
        final String jsonResult = jacksonSpan.toJsonString();
        final Map<String, Object> resultMap = mapper.readValue(jsonResult, new TypeReference<Map<String, Object>>() {});

        assertThat(resultMap.containsKey("key1"), is(true));
        assertThat(resultMap.containsKey("key2"), is(true));
        assertThat(resultMap.containsKey("attributes"), is(false));
    }

    @Test
    public void testToJsonStringWithoutAttributes() throws JsonProcessingException {
        builder.withAttributes(null);
        final String jsonResult = builder.build().toJsonString();
        final Map<String, Object> resultMap = mapper.readValue(jsonResult, new TypeReference<Map<String, Object>>() {});

        assertThat(resultMap.containsKey("key1"), is(false));
        assertThat(resultMap.containsKey("key2"), is(false));
        assertThat(resultMap.containsKey("attributes"), is(false));
    }
    
    @Test
    public void testBuilder_withAllParameters_createsSpan() {
        final JacksonSpan result = JacksonSpan.builder()
                .withSpanId(TEST_SPAN_ID)
                .withTraceId(TEST_TRACE_ID)
                .withTraceState(TEST_TRACE_STATE)
                .withParentSpanId(TEST_PARENT_SPAN_ID)
                .withName(TEST_NAME)
                .withServiceName(TEST_SERVICE_NAME)
                .withKind(TEST_KIND)
                .withStartTime(TEST_START_TIME)
                .withEndTime(TEST_END_TIME)
                .withAttributes(TEST_ATTRIBUTES)
                .withDroppedAttributesCount(TEST_DROPPED_ATTRIBUTES_COUNT)
                .withEvents(Arrays.asList(defaultSpanEvent))
                .withDroppedEventsCount(TEST_DROPPED_EVENTS_COUNT)
                .withLinks(Arrays.asList(defaultLink))
                .withDroppedLinksCount(TEST_DROPPED_LINKS_COUNT)
                .withTraceGroup(TEST_TRACE_GROUP)
                .withDurationInNanos(TEST_DURATION_IN_NANOS)
                .withTraceGroupFields(defaultTraceGroupFields)
                .build();

        assertThat(result, is(notNullValue()));
    }

    @Test
    public void testBuilder_fromSpan_createsSpan() {
        final JacksonSpan referenceSpan = JacksonSpan.builder()
                .withSpanId(TEST_SPAN_ID)
                .withTraceId(TEST_TRACE_ID)
                .withTraceState(TEST_TRACE_STATE)
                .withParentSpanId(TEST_PARENT_SPAN_ID)
                .withName(TEST_NAME)
                .withServiceName(TEST_SERVICE_NAME)
                .withKind(TEST_KIND)
                .withStartTime(TEST_START_TIME)
                .withEndTime(TEST_END_TIME)
                .withAttributes(TEST_ATTRIBUTES)
                .withDroppedAttributesCount(TEST_DROPPED_ATTRIBUTES_COUNT)
                .withEvents(Arrays.asList(defaultSpanEvent))
                .withDroppedEventsCount(TEST_DROPPED_EVENTS_COUNT)
                .withLinks(Arrays.asList(defaultLink))
                .withDroppedLinksCount(TEST_DROPPED_LINKS_COUNT)
                .withTraceGroup(TEST_TRACE_GROUP)
                .withDurationInNanos(TEST_DURATION_IN_NANOS)
                .withTraceGroupFields(defaultTraceGroupFields)
                .build();
        final JacksonSpan result = JacksonSpan.builder().fromSpan(referenceSpan).build();

        assertThat(result, is(notNullValue()));
        assertThat(result.getSpanId(), is(equalTo(referenceSpan.getSpanId())));
        assertThat(result.getTraceId(), is(equalTo(referenceSpan.getTraceId())));
        assertThat(result.getTraceState(), is(equalTo(referenceSpan.getTraceState())));
        assertThat(result.getParentSpanId(), is(equalTo(referenceSpan.getParentSpanId())));
        assertThat(result.getName(), is(equalTo(referenceSpan.getName())));
        assertThat(result.getDurationInNanos(), is(equalTo(referenceSpan.getDurationInNanos())));
        assertThat(result.getServiceName(), is(equalTo(referenceSpan.getServiceName())));
        assertThat(result.getKind(), is(equalTo(referenceSpan.getKind())));
        assertThat(result.getStartTime(), is(equalTo(referenceSpan.getStartTime())));
        assertThat(result.getEndTime(), is(equalTo(referenceSpan.getEndTime())));
        assertThat(result.getAttributes(), is(equalTo(referenceSpan.getAttributes())));
        assertThat(result.getDroppedAttributesCount(), is(equalTo(referenceSpan.getDroppedAttributesCount())));
        assertThat(result.getDroppedEventsCount(), is(equalTo(referenceSpan.getDroppedEventsCount())));
        assertThat(result.getDroppedLinksCount(), is(equalTo(referenceSpan.getDroppedLinksCount())));
        assertThat(result.getTraceGroup(), is(equalTo(referenceSpan.getTraceGroup())));
        assertThat(result.getTraceGroupFields(), is(equalTo(referenceSpan.getTraceGroupFields())));
        assertThat(result.getEvents(), is(equalTo(referenceSpan.getEvents())));
        assertThat(result.getLinks(), is(equalTo(referenceSpan.getLinks())));
    }

    @Test
    public void testBuilder_missingNonNullParameters_throwsNullPointerException() {
        final JacksonSpan.Builder builder = JacksonSpan.builder();
        builder.withTraceGroup(null);
        assertThrows(NullPointerException.class, builder::build);
    }
    
    @Test
    public void testBuilder_withoutTraceId_throwsNullPointerException() {
        builder.withTraceId(null);
        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    public void testBuilder_withEmptyTraceId_throwsIllegalArgumentException() {
        builder.withTraceId("");
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    public void testBuilder_withoutSpanId_throwsNullPointerException() {
        builder.withSpanId(null);
        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    public void testBuilder_withEmptySpanId_throwsIllegalArgumentException() {
        builder.withSpanId("");
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    public void testBuilder_withoutName_throwsNullPointerException() {
        builder.withName(null);
        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    public void testBuilder_withEmptyName_throwsIllegalArgumentException() {
        builder.withName("");
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    public void testBuilder_withoutKind_throwsNullPointerException() {
        builder.withKind(null);
        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    public void testBuilder_withEmptyKind_throwsIllegalArgumentException() {
        builder.withKind("");
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    public void testBuilder_withoutStartTime_throwsNullPointerException() {
        builder.withStartTime(null);
        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    public void testBuilder_withEmptyStartTime_throwsIllegalArgumentException() {
        builder.withStartTime("");
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    public void testBuilder_withoutEndTime_throwsNullPointerException() {
        builder.withEndTime(null);
        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    public void testBuilder_withEmptyEndTime_throwsIllegalArgumentException() {
        builder.withEndTime("");
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    public void testBuilder_missingTraceGroupKey_throwsIllegalStateException() {
        builder = JacksonSpan.builder()
                .withSpanId(TEST_SPAN_ID)
                .withTraceId(TEST_TRACE_ID)
                .withTraceState(TEST_TRACE_STATE)
                .withParentSpanId(TEST_PARENT_SPAN_ID)
                .withName(TEST_NAME)
                .withServiceName(TEST_SERVICE_NAME)
                .withKind(TEST_KIND)
                .withStartTime(TEST_START_TIME)
                .withEndTime(TEST_END_TIME)
                .withAttributes(TEST_ATTRIBUTES)
                .withDroppedAttributesCount(TEST_DROPPED_ATTRIBUTES_COUNT)
                .withEvents(Arrays.asList(defaultSpanEvent))
                .withDroppedEventsCount(TEST_DROPPED_EVENTS_COUNT)
                .withLinks(Arrays.asList(defaultLink))
                .withDroppedLinksCount(TEST_DROPPED_LINKS_COUNT)
                .withDurationInNanos(TEST_DURATION_IN_NANOS)
                .withTraceGroupFields(defaultTraceGroupFields);
        assertThrows(IllegalStateException.class, builder::build);
    }

    @Test
    public void testBuilder_withoutTraceGroupFields_throwsNullPointerException() {
        builder.withTraceGroupFields(null);
        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    public void testBuilder_allRequiredParameters_createsSpanWithDefaultValues() {

        final JacksonSpan span = JacksonSpan.builder()
                .withSpanId(TEST_SPAN_ID)
                .withTraceId(TEST_TRACE_ID)
                .withTraceState(TEST_TRACE_STATE)
                .withParentSpanId(TEST_PARENT_SPAN_ID)
                .withName(TEST_NAME)
                .withServiceName(TEST_SERVICE_NAME)
                .withKind(TEST_KIND)
                .withStartTime(TEST_START_TIME)
                .withEndTime(TEST_END_TIME)
                .withTraceGroup(TEST_TRACE_GROUP)
                .withDurationInNanos(TEST_DURATION_IN_NANOS)
                .withTraceGroupFields(defaultTraceGroupFields)
                .getThis()
                .build();

        assertThat(span, is(notNullValue()));
        assertThat(span.getAttributes(), is(equalTo(new HashMap<>())));
        assertThat(span.getDroppedAttributesCount(), is(equalTo(0)));
        assertThat(span.getEvents(), is(equalTo(new LinkedList<>())));
        assertThat(span.getDroppedEventsCount(), is(equalTo(0)));
        assertThat(span.getLinks(), is(equalTo(new LinkedList<>())));
        assertThat(span.getDroppedLinksCount(), is(equalTo(0)));
    }

    @Test
    public void testBuilder_withNullAttributes_createsSpanWithDefaultValue() {
        final JacksonSpan span = builder.withAttributes(null).build();
        assertThat(span.getAttributes(), is(equalTo(new HashMap<>())));
    }

    @Test
    public void testBuilder_withNullDroppedAttributesCount_createsSpanWithDefaultValue() {
        final JacksonSpan span = builder.withDroppedAttributesCount(null).build();
        assertThat(span.getDroppedAttributesCount(), is(equalTo(0)));
    }

    @Test
    public void testBuilder_withNullEvents_createsSpanWithDefaultValue() {
        final JacksonSpan span = builder.withEvents(null).build();
        assertThat(span.getEvents(), is(equalTo(new LinkedList<>())));
    }

    @Test
    public void testBuilder_withNullDroppedEventsCount_createsSpanWithDefaultValue() {
        final JacksonSpan span = builder.withDroppedEventsCount(null).build();
        assertThat(span.getDroppedEventsCount(), is(equalTo(0)));
    }

    @Test
    public void testBuilder_withNullLinks_createsSpanWithDefaultValue() {
        final JacksonSpan span = builder.withLinks(null).build();
        assertThat(span.getLinks(), is(equalTo(new LinkedList<>())));
    }

    @Test
    public void testBuilder_withNullDroppedLinksCount_createsSpanWithDefaultValue() {
        final JacksonSpan span = builder.withDroppedLinksCount(null).build();
        assertThat(span.getDroppedLinksCount(), is(equalTo(0)));
    }

    @Test
    public void testBuilder_missingRequiredParameters_throwsNullPointerException() {

        final JacksonEvent.Builder builder = JacksonSpan.builder()
                .withSpanId(TEST_SPAN_ID)
                .withTraceId(TEST_TRACE_ID)
                .withTraceState(TEST_TRACE_STATE)
                .withParentSpanId(TEST_PARENT_SPAN_ID)
                .withName(TEST_NAME)
                .withServiceName(TEST_SERVICE_NAME)
                .withStartTime(TEST_START_TIME)
                .withEndTime(TEST_END_TIME)
                .withTraceGroup(TEST_TRACE_GROUP)
                .withDurationInNanos(TEST_DURATION_IN_NANOS);

        assertThrows(NullPointerException.class, builder::build);
    }
}
