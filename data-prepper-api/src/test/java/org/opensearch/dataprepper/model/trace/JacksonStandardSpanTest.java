/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.trace;

import org.opensearch.dataprepper.model.event.EventMetadata;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.micrometer.core.instrument.util.IOUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Test;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import org.json.JSONException;
import org.skyscreamer.jsonassert.JSONAssert;

import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.List;
import java.util.UUID;
import java.util.Date;

public class JacksonStandardSpanTest extends JacksonSpanTest {
    private static final ObjectMapper mapper = new ObjectMapper();
    protected static final String TEST_KEY2 = UUID.randomUUID().toString();
    protected static final Long TEST_TIME_KEY1 = new Date().getTime();
    private static final Map<String, Object> TEST_ATTRIBUTES = ImmutableMap.of("key1", TEST_TIME_KEY1, "key2", TEST_KEY2);
    private static final Map<String, Object> TEST_STATUS = ImmutableMap.of("statusKey1", "statusValue1", "statusKey2", 2);
    private static final Map<String, Object> TEST_SCOPE = ImmutableMap.of("name", "testScope", "version", "testVersion", "attributes", List.of(Map.of("scopeAttrKey1", "scopeAttrValue1", "scopeAttrKey2", 2222)));
    private static final Map<String, Object> TEST_RESOURCE = ImmutableMap.of("attributes", List.of(Map.of("resourceAttrKey1", "resourceValueKey1", "resourceAttrKey2", 2000)));
    
    private JacksonSpan jacksonSpan;

    private DefaultLink defaultLink;

    private DefaultSpanEvent defaultSpanEvent;

    private DefaultTraceGroupFields defaultTraceGroupFields;
    
    
    public JacksonSpan createObjectUnderTest(Map<String, Object> attributes) {
        defaultSpanEvent = DefaultSpanEvent.builder()
                .withName("spanEventName")
                .withTime("spanEventTime")
                .build();

        defaultLink = DefaultLink.builder()
                .withTraceId("linkTraceId")
                .withSpanId("linkSpanId")
                .withTraceState("linkTraceState")
                .build();

        defaultTraceGroupFields = DefaultTraceGroupFields.builder()
                .withDurationInNanos(123L)
                .withStatusCode(201)
                .withEndTime("the End")
                .build();
        
        return  JacksonStandardSpan.builder()
                .withSpanId("testSpanId")
                .withTraceId("testTraceId")
                .withTraceState("testTraceState")
                .withParentSpanId("testParentSpanId")
                .withName("testName")
                .withServiceName("testService")
                .withKind("testKind")
                .withScope(TEST_SCOPE)
                .withResource(TEST_RESOURCE)
                .withStatus(TEST_STATUS)
                .withStartTime("testStartTime")
                .withEndTime("testEndTime")
                .withAttributes(attributes)
                .withDroppedAttributesCount(4)
                .withEvents(Arrays.asList(defaultSpanEvent))
                .withDroppedEventsCount(5)
                .withLinks(Arrays.asList(defaultLink))
                .withDroppedLinksCount(6)
                .withTraceGroup("testTraceGroup")
                .withDurationInNanos(123456L)
                .withTraceGroupFields(defaultTraceGroupFields)
                .build();
    }

    @Test
    @Override
    public void testGetTraceGroup() {
        jacksonSpan = createObjectUnderTest(TEST_ATTRIBUTES);
        EventMetadata metadata = jacksonSpan.getMetadata();
        String testTraceGroup = UUID.randomUUID().toString();
        metadata.setAttribute(JacksonSpan.TRACE_GROUP_KEY, testTraceGroup);
        final String traceGroup = jacksonSpan.getTraceGroup();
        assertThat(traceGroup, is(equalTo(testTraceGroup)));
    }

    @Test
    @Override
    public void testGetServiceName() {
        jacksonSpan = createObjectUnderTest(TEST_ATTRIBUTES);
        EventMetadata metadata = jacksonSpan.getMetadata();
        String testServiceName = UUID.randomUUID().toString();
        metadata.setAttribute(JacksonSpan.SERVICE_NAME_KEY, testServiceName);
        final String serviceName = jacksonSpan.getServiceName();
        assertThat(serviceName, is(equalTo(testServiceName)));
    }

    @Test
    @Override
    public void testSetAndGetServiceName() {
        jacksonSpan = createObjectUnderTest(TEST_ATTRIBUTES);
        String serviceName = jacksonSpan.getServiceName();
        assertThat(serviceName, is(equalTo("testService")));

        EventMetadata metadata = jacksonSpan.getMetadata();
        String testServiceName = UUID.randomUUID().toString();
        metadata.setAttribute(JacksonSpan.SERVICE_NAME_KEY, testServiceName);
        jacksonSpan.setServiceName(jacksonSpan.getServiceName());
        serviceName = jacksonSpan.getServiceName();
        assertThat(serviceName, is(equalTo(testServiceName)));
    }


    @Test
    @Override
    public void testGetTraceGroupFields() {
        jacksonSpan = createObjectUnderTest(TEST_ATTRIBUTES);
        DefaultTraceGroupFields testTraceGroupFields =
                DefaultTraceGroupFields.builder()
                .withDurationInNanos(10000L)
                .withEndTime(Instant.now().toString())
                .withStatusCode(10)
                .build();
        EventMetadata metadata = jacksonSpan.getMetadata();
        metadata.setAttribute(JacksonSpan.TRACE_GROUP_FIELDS_KEY, testTraceGroupFields);
        final TraceGroupFields traceGroupFields = jacksonSpan.getTraceGroupFields();
        assertThat(traceGroupFields, is(equalTo(testTraceGroupFields)));
    }

    @Test
    @Override
    public void testToJsonStringAllParameters() throws JsonProcessingException, JSONException {
        jacksonSpan = createObjectUnderTest(TEST_ATTRIBUTES);
        assertThat(jacksonSpan, instanceOf(JacksonStandardSpan.class));
        final String actual = jacksonSpan.toJsonString();
        String file = IOUtils.toString(this.getClass().getResourceAsStream("/testjson/standard_span.json"));
        String expected = String.format(file, TEST_TIME_KEY1, TEST_KEY2);
        JSONAssert.assertEquals(expected, actual, false);
    }

    @Test
    @Override
    public void testToJsonStringWithoutAttributes() throws JsonProcessingException {
        jacksonSpan = createObjectUnderTest(null);
        final String jsonResult = jacksonSpan.toJsonString();
        final Map<String, Object> resultMap = mapper.readValue(jsonResult, new TypeReference<Map<String, Object>>() {});

        assertThat(resultMap.containsKey("attributes"), is(true));
        assertThat(resultMap.get("attributes"), equalTo(Map.of()));
    }

}

