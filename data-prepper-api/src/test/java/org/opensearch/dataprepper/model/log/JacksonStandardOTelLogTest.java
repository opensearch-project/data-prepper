/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.log;

import com.google.common.collect.ImmutableMap;
import io.micrometer.core.instrument.util.IOUtils;
import org.junit.jupiter.api.Test;
import java.util.Map;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.equalTo;
import org.json.JSONException;
import org.skyscreamer.jsonassert.JSONAssert;

public class JacksonStandardOTelLogTest extends JacksonOtelLogTest {
    static final Map<String, Object> TEST_SCOPE = ImmutableMap.of("name", "testScope", "version", "testVersion", "attributes", List.of(Map.of("scopeAttrKey1", "scopeAttrValue1", "scopeAttrKey2", 2222)));
    static final Map<String, Object> TEST_RESOURCE = ImmutableMap.of("attributes", List.of(Map.of("resourceAttrKey1", "resourceAttrValue1", "resourceAttrKey2", 2000)));

    @Test
    @Override
    public void testLogToJsonString() throws JSONException {
        JacksonOtelLog log = JacksonStandardOTelLog.builder()
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
                .withBody(TEST_BODY)
                .build();

        final String actual = log.toJsonString();
        String file = IOUtils.toString(this.getClass().getResourceAsStream("/testjson/standard_log.json"));
        String expected = String.format(file, TEST_TIME_KEY1, TEST_KEY2);
        JSONAssert.assertEquals(expected, actual, false);
    }

    @Test
    @Override
    public void test_non_object_attributes_toJsonString_serializes_as_is() {
        JacksonOtelLog testLog = JacksonStandardOTelLog.builder()
                .withAttributes(Map.of("key", "value"))
                .build();
        assertThat(testLog, instanceOf(JacksonStandardOTelLog.class));
        assertThat(testLog.toJsonString(), equalTo("{\"attributes\":{\"key\":\"value\"}}"));

        testLog.put("attributes", "a string");
        assertThat(testLog.toJsonString(), equalTo("{\"attributes\":\"a string\"}"));
    }
}
