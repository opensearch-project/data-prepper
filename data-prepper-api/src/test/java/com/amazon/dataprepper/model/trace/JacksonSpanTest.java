package com.amazon.dataprepper.model.trace;

import com.amazon.dataprepper.model.event.JacksonEvent;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

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

@ExtendWith(MockitoExtension.class)
public class JacksonSpanTest {

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

    private JacksonSpan.Builder builder;
    
    private JacksonSpan jacksonSpan;

    @Mock(serializable = true)
    private DefaultLink defaultLink;

    @Mock(serializable = true)
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
    public void testBuilder() {
        final JacksonSpan result = builder.build();

        assertThat(result, is(notNullValue()));
    }
    
    @Test
    public void testBuilder_withoutTraceId_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> builder.withTraceId(null));
    }

    @Test
    public void testBuilder_withEmptyTraceId_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> builder.withTraceId(""));
    }

    @Test
    public void testBuilder_withoutSpanId_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> builder.withSpanId(null));
    }

    @Test
    public void testBuilder_withEmptySpanId_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> builder.withSpanId(""));
    }

    @Test
    public void testBuilder_withoutTraceState_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> builder.withTraceState(null));
    }

    @Test
    public void testBuilder_withEmptyTraceState_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> builder.withTraceState(""));
    }

    @Test
    public void testBuilder_withoutParentSpanId_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> builder.withParentSpanId(null));
    }

    @Test
    public void testBuilder_withEmptyParentSpanId_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> builder.withParentSpanId(""));
    }

    @Test
    public void testBuilder_withoutName_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> builder.withName(null));
    }

    @Test
    public void testBuilder_withEmptyName_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> builder.withName(""));
    }

    @Test
    public void testBuilder_withoutKind_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> builder.withKind(null));
    }

    @Test
    public void testBuilder_withEmptyKind_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> builder.withKind(""));
    }

    @Test
    public void testBuilder_withoutStartTime_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> builder.withStartTime(null));
    }

    @Test
    public void testBuilder_withEmptyStartTime_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> builder.withStartTime(""));
    }

    @Test
    public void testBuilder_withoutEndTime_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> builder.withEndTime(null));
    }

    @Test
    public void testBuilder_withEmptyEndTime_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> builder.withEndTime(""));
    }

    @Test
    public void testBuilder_withoutTraceGroup_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> builder.withTraceGroup(null));
    }

    @Test
    public void testBuilder_withEmptyTraceGroup_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> builder.withTraceGroup(""));
    }

    @Test
    public void testBuilder_allRequiredParameters() {

        final JacksonSpan span = JacksonSpan.builder()
                .withSpanId(TEST_SPAN_ID)
                .withTraceId(TEST_TRACE_ID)
                .withTraceState(TEST_TRACE_STATE)
                .withParentSpanId(TEST_PARENT_SPAN_ID)
                .withName(TEST_NAME)
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
    public void testBuilder_missingRequiredParameters() {

        final JacksonEvent.Builder builder = JacksonSpan.builder()
                .withSpanId(TEST_SPAN_ID)
                .withTraceId(TEST_TRACE_ID)
                .withTraceState(TEST_TRACE_STATE)
                .withParentSpanId(TEST_PARENT_SPAN_ID)
                .withName(TEST_NAME)
                .withStartTime(TEST_START_TIME)
                .withEndTime(TEST_END_TIME)
                .withTraceGroup(TEST_TRACE_GROUP)
                .withDurationInNanos(TEST_DURATION_IN_NANOS);

        assertThrows(IllegalArgumentException.class, builder::build);
    }
}
