/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.trace;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.EventType;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.event.DefaultEventHandle;
import org.json.JSONException;

import java.time.Instant;
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
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JacksonSpanTest {
    private static final ObjectMapper mapper = new ObjectMapper();

    protected static final String TEST_TRACE_ID =  UUID.randomUUID().toString();
    protected static final String TEST_SPAN_ID =  UUID.randomUUID().toString();
    protected static final String TEST_TRACE_STATE =  UUID.randomUUID().toString();
    protected static final String TEST_PARENT_SPAN_ID =  UUID.randomUUID().toString();
    protected static final String TEST_NAME =  UUID.randomUUID().toString();
    protected static final String TEST_KIND =  UUID.randomUUID().toString();
    protected static final String TEST_START_TIME =  UUID.randomUUID().toString();
    protected static final String TEST_END_TIME =  UUID.randomUUID().toString();
    private static final Map<String, Object> TEST_ATTRIBUTES = ImmutableMap.of("key1", new Date().getTime(), "key2", UUID.randomUUID().toString());
    private static final Map<String, Object> TEST_STATUS = ImmutableMap.of("statusKey1", UUID.randomUUID().toString(), "statusKey2", UUID.randomUUID().toString());
    private static final Map<String, Object> TEST_SCOPE = ImmutableMap.of("name", UUID.randomUUID().toString(), "version", UUID.randomUUID().toString(), "attributes", List.of(Map.of("key", UUID.randomUUID().toString(), "value", UUID.randomUUID().toString())));
    private static final Map<String, Object> TEST_RESOURCE = ImmutableMap.of("attributes", List.of(Map.of("key", UUID.randomUUID().toString(), "value", UUID.randomUUID().toString())));
    protected static final Integer TEST_DROPPED_ATTRIBUTES_COUNT = 8;
    protected static final Integer TEST_DROPPED_EVENTS_COUNT =  45;
    protected static final Integer TEST_DROPPED_LINKS_COUNT =  21;
    protected static final String TEST_TRACE_GROUP =  UUID.randomUUID().toString();
    protected static final Long TEST_DURATION_IN_NANOS =  537L;
    protected static final String TEST_SERVICE_NAME = UUID.randomUUID().toString();

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
                .withScope(TEST_SCOPE)
                .withResource(TEST_RESOURCE)
                .withStatus(TEST_STATUS)
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
    public void testGetScope() {
        final Map<String, Object> scope = jacksonSpan.getScope();
        assertThat(scope, is(equalTo(TEST_SCOPE)));
    }

    @Test
    public void testGetResource() {
        final Map<String, Object> resource = jacksonSpan.getResource();
        assertThat(resource, is(equalTo(TEST_RESOURCE)));
    }

    @Test
    public void testGetStatus() {
        final Map<String, Object> status = jacksonSpan.getStatus();
        assertThat(status, is(equalTo(TEST_STATUS)));
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
    public void testToJsonStringAllParameters() throws JsonProcessingException, JSONException {
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
    public void testGetTimeReceived() {
        Instant now = Instant.now();
        final JacksonSpan span = builder.withTimeReceived(now).build();
        assertThat(((DefaultEventHandle)span.getEventHandle()).getInternalOriginationTime(), is(now));
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

    @Nested
    class JacksonSpanBuilder {
        @Test
        void testWithJsonData_with_valid_json_data() {
            final String data = "{\n" +
                    "  \"traceId\": \"414243\",\n" +
                    "  \"droppedLinksCount\": 0,\n" +
                    "  \"kind\": \"SPAN_KIND_INTERNAL\",\n" +
                    "  \"droppedEventsCount\": 0,\n" +
                    "  \"traceGroupFields\": {\n" +
                    "    \"endTime\": \"1970-01-01T00:00:00Z\",\n" +
                    "    \"durationInNanos\": 0,\n" +
                    "    \"statusCode\": 0\n" +
                    "  },\n" +
                    "  \"traceGroup\": \"FRUITS\",\n" +
                    "  \"serviceName\": \"ServiceA\",\n" +
                    "  \"parentSpanId\": \"\",\n" +
                    "  \"spanId\": \"313030\",\n" +
                    "  \"traceState\": \"\",\n" +
                    "  \"name\": \"FRUITS\",\n" +
                    "  \"startTime\": \"1970-01-01T00:00:00Z\",\n" +
                    "  \"links\": [],\n" +
                    "  \"endTime\": \"1970-01-01T00:00:00Z\",\n" +
                    "  \"droppedAttributesCount\": 0,\n" +
                    "  \"durationInNanos\": 0,\n" +
                    "  \" events\": [],\n" +
                    "  \"resource.attributes.service@name\": \"ServiceA\",\n" +
                    "  \"status.code\": 0\n" +
                    "}";
            final JacksonSpan jacksonSpan = JacksonSpan.builder()
                    .withJsonData(data)
                    .build();

            assertThat(jacksonSpan, is(notNullValue()));
            assertThat(jacksonSpan.getTraceId(), equalTo("414243"));
            assertThat(jacksonSpan.getSpanId(), equalTo("313030"));
            assertThat(jacksonSpan.getTraceGroup(), equalTo("FRUITS"));
            assertThat(jacksonSpan.getKind(), equalTo("SPAN_KIND_INTERNAL"));
            assertThat(jacksonSpan.getEndTime(), equalTo("1970-01-01T00:00:00Z"));
        }

        @Test
        void testBuilder_withJsonData_missingTraceGroupKey_throwsIllegalStateException() {
            final String object = "{\"traceId\": \"414243\"}";
            final JacksonSpan.Builder builder = JacksonSpan.builder()
                    .withJsonData(object);

            assertThrows(IllegalStateException.class, builder::build);
        }

        @Test
        void testBuilder_withJsonData_missing_non_empty_keys_throwsNullPointerException() {
            final String object = "{\"traceGroup\": \"FRUITS\"}";
            final JacksonSpan.Builder builder = JacksonSpan.builder()
                    .withJsonData(object);

            assertThrows(NullPointerException.class, builder::build);
        }

        @Test
        void testBuilder_withJsonData_with_empty_string_for_non_empty_key_throwsIllegalArgumentException() {
            final String object = "{\n" +
                    "  \"traceGroup\": \"FRUITS\",\n" +
                    "  \"traceId\": \"\" \n" +
                    "}";

            final JacksonSpan.Builder builder = JacksonSpan.builder()
                    .withJsonData(object);

            assertThrows(IllegalArgumentException.class, builder::build);
        }

        @Test
        void testBuilder_withJsonData_with_null_non_null_key_throwsNullPointerException() {
            final String object = "{\n" +
                    "  \"traceId\": \"414243\",\n" +
                    "  \"traceGroup\": \"FRUITS\",\n" +
                    "  \"kind\": \"SPAN_KIND_INTERNAL\",\n" +
                    "  \"spanId\": \"313030\",\n" +
                    "  \"name\": \"FRUITS\",\n" +
                    "  \"startTime\": \"1970-01-01T00:00:00Z\",\n" +
                    "  \"endTime\": \"1970-01-01T00:00:00Z\" " +
                    "}";

            final JacksonSpan.Builder builder = JacksonSpan.builder()
                    .withJsonData(object);

            assertThrows(NullPointerException.class, builder::build);
        }

        @Test
        void testBuilder_withJsonData_with_invalid_json_data_should_throw() {
            String invalidJsonData = "{\"traceGroup\": \"FRUITS}";
            final JacksonSpan.Builder builder = JacksonSpan.builder();

            assertThrows(RuntimeException.class, () -> builder.withJsonData(invalidJsonData));
        }

        @Test
        void testBuilder_withEventMetadata_with_event_metadata_with_valid_metadata() {
            final String data = "{\n" +
                    "  \"traceId\": \"414243\",\n" +
                    "  \"kind\": \"SPAN_KIND_INTERNAL\",\n" +
                    "  \"traceGroupFields\": {\n" +
                    "    \"endTime\": \"1970-01-01T00:00:00Z\",\n" +
                    "    \"durationInNanos\": 0,\n" +
                    "    \"statusCode\": 0\n" +
                    "  },\n" +
                    "  \"traceGroup\": \"FRUITS\",\n" +
                    "  \"spanId\": \"313030\",\n" +
                    "  \"name\": \"FRUITS\",\n" +
                    "  \"startTime\": \"1970-01-01T00:00:00Z\",\n" +
                    "  \"endTime\": \"1970-01-01T00:00:00Z\",\n" +
                    "  \"durationInNanos\": 0" +
                    "}";

            EventMetadata eventMetadata = mock(EventMetadata.class);
            final Instant now = Instant.now();
            when(eventMetadata.getEventType()).thenReturn(String.valueOf(EventType.TRACE));
            when(eventMetadata.getTimeReceived()).thenReturn(now);

            final JacksonSpan jacksonSpan = JacksonSpan.builder()
                    .withJsonData(data)
                    .withEventMetadata(eventMetadata)
                    .build();

            assertThat(jacksonSpan, is(notNullValue()));
            assertThat(jacksonSpan.getMetadata(), is(notNullValue()));
            assertThat(jacksonSpan.getMetadata().getTimeReceived(), equalTo(now));
        }

        @Test
        void testBuilder_withEventMetadata_with_event_invalid_event_metadata_should_throw() {
            final String data = "{\n" +
                    "  \"traceId\": \"414243\",\n" +
                    "  \"kind\": \"SPAN_KIND_INTERNAL\",\n" +
                    "  \"traceGroupFields\": {\n" +
                    "    \"endTime\": \"1970-01-01T00:00:00Z\",\n" +
                    "    \"durationInNanos\": 0,\n" +
                    "    \"statusCode\": 0\n" +
                    "  },\n" +
                    "  \"traceGroup\": \"FRUITS\",\n" +
                    "  \"spanId\": \"313030\",\n" +
                    "  \"name\": \"FRUITS\",\n" +
                    "  \"startTime\": \"1970-01-01T00:00:00Z\",\n" +
                    "  \"endTime\": \"1970-01-01T00:00:00Z\",\n" +
                    "  \"durationInNanos\": 0" +
                    "}";

            EventMetadata eventMetadata = mock(EventMetadata.class);
            final Instant now = Instant.now();
            when(eventMetadata.getEventType()).thenReturn(String.valueOf(EventType.LOG));
            when(eventMetadata.getTimeReceived()).thenReturn(now);

            final JacksonEvent.Builder builder = JacksonSpan.builder()
                    .withJsonData(data)
                    .withEventMetadata(eventMetadata);

            assertThrows(IllegalArgumentException.class, builder::build);
        }

        @Test
        void testBuilder_withData_with_event_valid_data() {
	    final Map<String, Object> data = new HashMap<String, Object>();
	    final String traceId = "414243";
	    final String kind = "SPAN_KIND_INTERNAL";
	    final String traceGroup = "FRUITSGroup";
	    final String traceGroupFields = "{\"endTime\":\"1970-01-01T00:00:00Z\",\"durationInNanos\": 0,\"statusCode\": 0}";
	    final String spanId = "313030";
	    final String name = "FRUITS";
	    final String startTime = "1970-01-01T00:00:00Z";
	    final String endTime = "1970-01-02T00:00:00Z";
	    final String durationInNanos = "100";
	    data.put("traceId", traceId);
	    data.put("kind", kind);
	    data.put("traceGroup", traceGroup);
	    data.put("traceGroupFields", traceGroupFields);
	    data.put("spanId", spanId);
	    data.put("name", name);
	    data.put("startTime", startTime);
	    data.put("endTime", endTime);
	    data.put("durationInNanos", durationInNanos);

            EventMetadata eventMetadata = mock(EventMetadata.class);
            final Instant now = Instant.now();
            when(eventMetadata.getEventType()).thenReturn(String.valueOf(EventType.TRACE));
            when(eventMetadata.getTimeReceived()).thenReturn(now);


            final JacksonSpan jacksonSpan = JacksonSpan.builder()
                    .withData(data)
                    .withEventMetadata(eventMetadata)
                    .build();

            assertThat(jacksonSpan, is(notNullValue()));
            assertThat(jacksonSpan.getMetadata(), is(notNullValue()));
            assertThat(jacksonSpan.getMetadata().getTimeReceived(), equalTo(now));
            assertThat(jacksonSpan.toMap().get("traceId"), equalTo(traceId));
            assertThat(jacksonSpan.toMap().get("kind"), equalTo(kind));
            assertThat(jacksonSpan.toMap().get("traceGroup"), equalTo(traceGroup));
            assertThat(jacksonSpan.toMap().get("spanId"), equalTo(spanId));
            assertThat(jacksonSpan.toMap().get("name"), equalTo(name));
            assertThat(jacksonSpan.toMap().get("startTime"), equalTo(startTime));
            assertThat(jacksonSpan.toMap().get("endTime"), equalTo(endTime));
            assertThat(jacksonSpan.toMap().get("durationInNanos"), equalTo(durationInNanos));
        }
    }

    @Test
    void fromSpan_with_a_Jackson_Span() {
        final JacksonEvent createdEvent = JacksonSpan.fromSpan(jacksonSpan);

        assertThat(createdEvent, notNullValue());
        assertThat(createdEvent, not(sameInstance(jacksonSpan)));

        assertThat(createdEvent.toMap(), equalTo(jacksonSpan.toMap()));

        assertThat(createdEvent.getMetadata(), notNullValue());
        assertThat(createdEvent.getMetadata(), not(sameInstance(jacksonSpan.getMetadata())));
        assertThat(createdEvent.getMetadata(), equalTo(jacksonSpan.getMetadata()));
    }

    @Test
    void fromSpan_with_a_non_JacksonSpan() {
        final EventMetadata eventMetadata = mock(EventMetadata.class);
        final Span originalSpan = mock(Span.class);
        when(originalSpan.toMap()).thenReturn(jacksonSpan.toMap());
        when(originalSpan.getMetadata()).thenReturn(eventMetadata);
        when(eventMetadata.getEventType()).thenReturn("TRACE");

        final JacksonSpan createdEvent = JacksonSpan.fromSpan(originalSpan);

        assertThat(createdEvent, notNullValue());

        assertThat(createdEvent.toMap(), equalTo(jacksonSpan.toMap()));

        assertThat(createdEvent.getMetadata(), notNullValue());
        assertThat(createdEvent.getMetadata(), not(sameInstance(jacksonSpan.getMetadata())));
        assertThat(createdEvent.getMetadata().getEventType(), equalTo("TRACE"));
    }
}
