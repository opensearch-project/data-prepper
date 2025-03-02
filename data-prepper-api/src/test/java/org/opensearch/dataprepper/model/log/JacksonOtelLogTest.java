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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
import static org.hamcrest.Matchers.not;

public class JacksonOtelLogTest {

    private static final String TEST_KEY2 = UUID.randomUUID().toString();
    private static final Long TEST_TIME_KEY1 = new Date().getTime();
    private static final Map<String, Object> TEST_ATTRIBUTES = ImmutableMap.of(
            "key1", TEST_TIME_KEY1,
            "key2", TEST_KEY2);

    private static final Map<String, Object> TEST_SCOPE = ImmutableMap.of("name", UUID.randomUUID().toString(), "version", UUID.randomUUID().toString(), "attributes", List.of(Map.of("key", UUID.randomUUID().toString(), "value", UUID.randomUUID().toString())));
    private static final Map<String, Object> TEST_RESOURCE = ImmutableMap.of("attributes", List.of(Map.of("key", UUID.randomUUID().toString(), "value", UUID.randomUUID().toString())));
    private static final String TEST_SERVICE_NAME = "service";
    private static final String TEST_OBSERVED_TIME = "2022-01-01T00:00:00Z";
    private static final String TEST_TIME = "2022-01-02T00:00:00Z";
    private static final String TEST_SCHEMA_URL = "schema";
    private static final Integer TEST_FLAGS = 1;
    private static final String TEST_TRACE_ID = "1234";
    private static final String TEST_SPAN_ID = "4321";
    private static final Integer TEST_SEVERITY_NUMBER = 2;
    private static final String TEST_SEVERITY_TEXT = "severity";
    private static final Integer TEST_DROPPED_ATTRIBUTES_COUNT = 4;
    private static final Object TEST_BODY = Map.of("log", "message");

    private JacksonOtelLog log;
    private JacksonOtelLog.Builder builder;

    @BeforeEach
    public void setup() {
        createObjectUnderTest(true);
    }

    private JacksonOtelLog.Builder createBuilder(final boolean opensearchMode) {
        builder = JacksonOtelLog.builder(opensearchMode)
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
        return builder;
    }

    private JacksonOtelLog createObjectUnderTest(final boolean opensearchMode) {
        createBuilder(opensearchMode);
        log = builder.build();
        return log;
    }

    @Test
    public void testGetTime() {
        final String time = log.getTime();
        assertThat(time, is(equalTo(TEST_TIME)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testGetObservedTime(final boolean opensearchMode) {
        log = createObjectUnderTest(opensearchMode);
        final String observedTime = log.getObservedTime();
        assertThat(observedTime, is(equalTo(TEST_OBSERVED_TIME)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testGetServiceName(final boolean opensearchMode) {
        log = createObjectUnderTest(opensearchMode);
        final String name = log.getServiceName();
        assertThat(name, is(equalTo(TEST_SERVICE_NAME)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testGetScope(final boolean opensearchMode) {
        log = createObjectUnderTest(opensearchMode);
        final Map<String, Object> scope = log.getScope();
        assertThat(scope, is(equalTo(TEST_SCOPE)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testGetResource(final boolean opensearchMode) {
        log = createObjectUnderTest(opensearchMode);
        final Map<String, Object> resource = log.getResource();
        assertThat(resource, is(equalTo(TEST_RESOURCE)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testOpensearchMode(final boolean opensearchMode) {
        log = createObjectUnderTest(opensearchMode);
        boolean mode = log.getOpensearchMode();
        assertThat(mode, is(equalTo(opensearchMode)));
        log.setOpensearchMode(!opensearchMode);
        mode = log.getOpensearchMode();
        assertThat(mode, is(equalTo(!opensearchMode)));
    }

    @Test
    public void testGetTimeReceived() {
        Instant now = Instant.now();
        builder.withTimeReceived(now);
        log = builder.build();
        assertThat(((DefaultEventHandle)log.getEventHandle()).getInternalOriginationTime(), is(now));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testGetSchemaUrl(final boolean opensearchMode) {
        log = createObjectUnderTest(opensearchMode);
        final String schemaUrl = log.getSchemaUrl();
        assertThat(schemaUrl, is(equalTo(TEST_SCHEMA_URL)));
    }

    @Test
    public void testGetFlags() {
        final Integer flags = log.getFlags();
        assertThat(flags, is(equalTo(TEST_FLAGS)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testGetTraceId(final boolean opensearchMode) {
        log = createObjectUnderTest(opensearchMode);
        final String traceId = log.getTraceId();
        assertThat(traceId, is(equalTo(TEST_TRACE_ID)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testGetSpanId(final boolean opensearchMode) {
        log = createObjectUnderTest(opensearchMode);
        final String spanId = log.getSpanId();
        assertThat(spanId, is(equalTo(TEST_SPAN_ID)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testGetServerityText(final boolean opensearchMode) {
        log = createObjectUnderTest(opensearchMode);
        final String severityText = log.getSeverityText();
        assertThat(severityText, is(equalTo(TEST_SEVERITY_TEXT)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testGetServerityNumber(final boolean opensearchMode) {
        log = createObjectUnderTest(opensearchMode);
        final Integer observedTime = log.getSeverityNumber();
        assertThat(observedTime, is(equalTo(TEST_SEVERITY_NUMBER)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testGetDroppedAttributesCount(final boolean opensearchMode) {
        log = createObjectUnderTest(opensearchMode);
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
    public void testHistogramToJsonString() throws JSONException {
        final String actual = log.toJsonString();
        String file = IOUtils.toString(this.getClass().getResourceAsStream("/testjson/log.json"));
        String expected = String.format(file, TEST_TIME_KEY1, TEST_KEY2);
        JSONAssert.assertEquals(expected, actual, false);
    }

    @Test
    public void testHistogramToJsonStringNoOpensearchMode() throws JSONException {
        log = createObjectUnderTest(false);
        final String str = log.toJsonString();
        assertThat(str.indexOf(JacksonOtelLog.OTLP_SERVICE_NAME_KEY), not(equalTo(-1)));
        assertThat(str.indexOf(JacksonOtelLog.OTLP_OBSERVED_TIME_KEY), not(equalTo(-1)));
        assertThat(str.indexOf(JacksonOtelLog.OTLP_SCHEMA_URL_KEY), not(equalTo(-1)));
        assertThat(str.indexOf(JacksonOtelLog.OTLP_TRACE_ID_KEY), not(equalTo(-1)));
        assertThat(str.indexOf(JacksonOtelLog.OTLP_SPAN_ID_KEY), not(equalTo(-1)));
        
    }

    @Test
    public void test_non_object_attributes_toJsonString_serializes_as_is() {
        JacksonOtelLog testLog = JacksonOtelLog.builder(true)
                .withAttributes(Map.of("key", "value"))
                .build();
        assertThat(testLog.toJsonString(), equalTo("{\"key\":\"value\"}"));

        testLog.put("attributes", "a string");
        assertThat(testLog.toJsonString(), equalTo("{\"attributes\":\"a string\"}"));
    }
}

