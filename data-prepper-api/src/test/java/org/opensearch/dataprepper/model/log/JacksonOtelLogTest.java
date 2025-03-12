/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.log;

import com.google.common.collect.ImmutableMap;
import io.micrometer.core.instrument.util.IOUtils;
import org.json.JSONException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.opensearch.dataprepper.model.event.DefaultEventHandle;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasValue;
import static org.hamcrest.Matchers.is;

public class JacksonOtelLogTest {

    protected static final String TEST_KEY2 = UUID.randomUUID().toString();
    protected static final Long TEST_TIME_KEY1 = new Date().getTime();
    protected static final Map<String, Object> TEST_ATTRIBUTES = ImmutableMap.of(
            "key1", TEST_TIME_KEY1,
            "key2", TEST_KEY2);

    private static final Map<String, Object> TEST_SCOPE = ImmutableMap.of("name", UUID.randomUUID().toString(), "version", UUID.randomUUID().toString(), "attributes", List.of(Map.of("key", UUID.randomUUID().toString(), "value", UUID.randomUUID().toString())));
    private static final Map<String, Object> TEST_RESOURCE = ImmutableMap.of("attributes", List.of(Map.of("key", UUID.randomUUID().toString(), "value", UUID.randomUUID().toString())));

    protected static final String TEST_SERVICE_NAME = "service";
    protected static final String TEST_OBSERVED_TIME = "2022-01-01T00:00:00Z";
    protected static final String TEST_TIME = "2022-01-02T00:00:00Z";
    protected static final String TEST_SCHEMA_URL = "schema";
    protected static final Integer TEST_FLAGS = 1;
    protected static final String TEST_TRACE_ID = "1234";
    protected static final String TEST_SPAN_ID = "4321";
    protected static final Integer TEST_SEVERITY_NUMBER = 2;
    protected static final String TEST_SEVERITY_TEXT = "severity";
    protected static final Integer TEST_DROPPED_ATTRIBUTES_COUNT = 4;
    protected static final Object TEST_BODY = Map.of("log", "message");

    private JacksonOtelLog log;
    private JacksonOtelLog.Builder builder = JacksonOtelLog.builder();

    @BeforeEach
    public void setup() {
        builder = builder
                .withTime(TEST_TIME)
                .withObservedTime(TEST_OBSERVED_TIME)
                .withServiceName(TEST_SERVICE_NAME)
                .withAttributes(TEST_ATTRIBUTES)
                .withSchemaUrl(TEST_SCHEMA_URL)
                .withFlags(TEST_FLAGS)
                .withTraceId(TEST_TRACE_ID)
                .withSpanId(TEST_SPAN_ID)
                .withScope(TEST_SCOPE)
                .withResource(TEST_RESOURCE)
                .withSeverityNumber(TEST_SEVERITY_NUMBER)
                .withSeverityText(TEST_SEVERITY_TEXT)
                .withDroppedAttributesCount(TEST_DROPPED_ATTRIBUTES_COUNT)
                .withBody(TEST_BODY);

        log = builder.build();
    }

    @Test
    public void testGetTime() {
        final String time = log.getTime();
        assertThat(time, is(equalTo(TEST_TIME)));
    }

    @Test
    public void testGetObservedTime() {
        final String observedTime = log.getObservedTime();
        assertThat(observedTime, is(equalTo(TEST_OBSERVED_TIME)));
    }

    @Test
    public void testGetServiceName() {
        final String name = log.getServiceName();
        assertThat(name, is(equalTo(TEST_SERVICE_NAME)));
    }

    @Test
    public void testGetTimeReceived() {
        Instant now = Instant.now();
        builder.withTimeReceived(now);
        log = builder.build();
        assertThat(((DefaultEventHandle)log.getEventHandle()).getInternalOriginationTime(), is(now));
    }

    @Test
    public void testGetSchemaUrl() {
        final String schemaUrl = log.getSchemaUrl();
        assertThat(schemaUrl, is(equalTo(TEST_SCHEMA_URL)));
    }

    @Test
    public void testGetFlags() {
        final Integer flags = log.getFlags();
        assertThat(flags, is(equalTo(TEST_FLAGS)));
    }

    @Test
    public void testGetTraceId() {
        final String traceId = log.getTraceId();
        assertThat(traceId, is(equalTo(TEST_TRACE_ID)));
    }

    @Test
    public void testGetSpanId() {
        final String spanId = log.getSpanId();
        assertThat(spanId, is(equalTo(TEST_SPAN_ID)));
    }

    @Test
    public void testGetServerityText() {
        final String severityText = log.getSeverityText();
        assertThat(severityText, is(equalTo(TEST_SEVERITY_TEXT)));
    }

    @Test
    public void testGetServerityNumber() {
        final Integer observedTime = log.getSeverityNumber();
        assertThat(observedTime, is(equalTo(TEST_SEVERITY_NUMBER)));
    }

    @Test
    public void testGetDroppedAttributesCount() {
        final Integer droppedAttributesCount = log.getDroppedAttributesCount();
        assertThat(droppedAttributesCount, is(equalTo(TEST_DROPPED_ATTRIBUTES_COUNT)));
    }

    @Test
    public void testGetBody() {
        final Map<String, String> body = (Map<String, String>) log.getBody();
        assertThat(body, hasKey("log"));
        assertThat(body, hasValue("message"));
    }

    @Test
    public void testGetScope() {
        final Map<String, Object> scope = log.getScope();
        assertThat(scope, is(equalTo(TEST_SCOPE)));
    }

    @Test
    public void testGetResource() {
        final Map<String, Object> resource = log.getResource();
        assertThat(resource, is(equalTo(TEST_RESOURCE)));
    }

    @Test
    public void testGetAttributes() {
        final Map<String, Object> attributes = log.getAttributes();
        TEST_ATTRIBUTES.keySet().forEach(key -> {
                    assertThat(attributes, hasKey(key));
                    assertThat(attributes.get(key), is(equalTo(TEST_ATTRIBUTES.get(key))));
                }
        );
    }

    @Test
    public void testGetEmptyAttributes() {
        JacksonOtelLog log2 = builder.withAttributes(null).build();
        final Map<String, Object> attributes = log2.getAttributes();
        Assertions.assertTrue(attributes.isEmpty());
    }


    @Test
    public void testLogToJsonString() throws JSONException {
        final String actual = log.toJsonString();
        String file = IOUtils.toString(this.getClass().getResourceAsStream("/testjson/log.json"));
        String expected = String.format(file, TEST_TIME_KEY1, TEST_KEY2);
        JSONAssert.assertEquals(expected, actual, false);
    }

    @Test
    public void test_non_object_attributes_toJsonString_serializes_as_is() {
        JacksonOtelLog testLog = JacksonOtelLog.builder()
                .withAttributes(Map.of("key", "value"))
                .build();
        assertThat(testLog.toJsonString(), equalTo("{\"key\":\"value\"}"));

        testLog.put("attributes", "a string");
        assertThat(testLog.toJsonString(), equalTo("{\"attributes\":\"a string\"}"));
    }
}

