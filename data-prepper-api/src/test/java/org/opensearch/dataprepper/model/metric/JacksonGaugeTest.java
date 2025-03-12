/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.metric;

import com.google.common.collect.ImmutableMap;
import io.micrometer.core.instrument.util.IOUtils;
import org.json.JSONException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.TestObject;
import org.opensearch.dataprepper.model.event.DefaultEventHandle;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.AggregateEventHandle;
import org.skyscreamer.jsonassert.JSONAssert;

import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JacksonGaugeTest {

    protected static final String TEST_KEY2 = UUID.randomUUID().toString();
    protected static final Long TEST_TIME_KEY1 = new Date().getTime();
    protected static final Map<String, Object> TEST_ATTRIBUTES = ImmutableMap.of(
            "key1", TEST_TIME_KEY1,
            "key2", TEST_KEY2);
    protected static final Map<String, Object> TEST_SCOPE = ImmutableMap.of("name", UUID.randomUUID().toString(), "version", UUID.randomUUID().toString(), "attributes", List.of(Map.of("key", UUID.randomUUID().toString(), "value", UUID.randomUUID().toString())));
    protected static final Map<String, Object> TEST_RESOURCE = ImmutableMap.of("attributes", List.of(Map.of("key", UUID.randomUUID().toString(), "value", UUID.randomUUID().toString())));
    protected static final String TEST_SERVICE_NAME = "service";
    protected static final String TEST_NAME = "name";
    protected static final String TEST_DESCRIPTION = "description";
    protected static final String TEST_UNIT_NAME = "unit";
    protected static final String TEST_START_TIME = "2022-01-01T00:00:00Z";
    protected static final String TEST_TIME = "2022-01-02T00:00:00Z";
    protected static final String TEST_EVENT_KIND = Metric.KIND.GAUGE.name();
    protected static final Double TEST_VALUE = 1D;
    protected static final String TEST_SCHEMA_URL = "schema";
    protected static final Integer TEST_FLAGS = 1;

    protected static final List<Exemplar> TEST_EXEMPLARS = Arrays.asList(
            new DefaultExemplar("1970-01-01T00:00:00Z", 2.0, "xsdf", "abcd", Map.of("test", "value")),
            new DefaultExemplar("1971-01-01T00:00:00Z", 5.0, "xsdt", "asdf", Map.of("test", "value"))
    );

    private JacksonGauge gauge;

    private JacksonGauge.Builder builder;

    @BeforeEach
    public void setup() {
        builder = JacksonGauge.builder()
                .withAttributes(TEST_ATTRIBUTES)
                .withName(TEST_NAME)
                .withDescription(TEST_DESCRIPTION)
                .withEventKind(TEST_EVENT_KIND)
                .withStartTime(TEST_START_TIME)
                .withTime(TEST_TIME)
                .withUnit(TEST_UNIT_NAME)
                .withScope(TEST_SCOPE)
                .withResource(TEST_RESOURCE)
                .withValue(TEST_VALUE)
                .withServiceName(TEST_SERVICE_NAME)
                .withExemplars(TEST_EXEMPLARS)
                .withSchemaUrl(TEST_SCHEMA_URL)
                .withFlags(TEST_FLAGS);

        gauge = builder.build();

    }

    @Test
    public void testGetAttributes() {
        final Map<String, Object> attributes = gauge.getAttributes();
        TEST_ATTRIBUTES.keySet().forEach(key -> {
                    assertThat(attributes, hasKey(key));
                    assertThat(attributes.get(key), is(equalTo(TEST_ATTRIBUTES.get(key))));
                }
        );
    }

    @Test
    public void testGetName() {
        final String name = gauge.getName();
        assertThat(name, is(equalTo(TEST_NAME)));
    }

    @Test
    public void testGetDefaultEventHandle() {
        EventHandle eventHandle = new DefaultEventHandle(Instant.now());
        builder.withEventHandle(eventHandle);
        JacksonGauge gauge = builder.build();
        final EventHandle handle = gauge.getEventHandle();
        assertThat(handle, is(sameInstance(eventHandle)));
    }

    @Test
    public void testGetAggregateEventHandle() {
        EventHandle eventHandle = new AggregateEventHandle(Instant.now());
        builder.withEventHandle(eventHandle);
        JacksonGauge gauge = builder.build();
        final EventHandle handle = gauge.getEventHandle();
        assertThat(handle, is(sameInstance(eventHandle)));
    }

    @Test
    public void testGetTimeReceived() {
        Instant now = Instant.now();
        builder.withTimeReceived(now);
        gauge = builder.build();
        assertThat(((DefaultEventHandle)gauge.getEventHandle()).getInternalOriginationTime(), is(now));
    }

    @Test
    public void testGetDescription() {
        final String description = gauge.getDescription();
        assertThat(description, is(equalTo(TEST_DESCRIPTION)));
    }

    @Test
    public void testGetKind() {
        final String kind = gauge.getKind();
        assertThat(kind, is(equalTo(TEST_EVENT_KIND)));
    }

    @Test
    public void testGetServiceName() {
        final String name = gauge.getServiceName();
        assertThat(name, is(equalTo(TEST_SERVICE_NAME)));
    }

    @Test
    public void testGetStartTime() {
        final String GetStartTime = gauge.getStartTime();
        assertThat(GetStartTime, is(equalTo(TEST_START_TIME)));
    }

    @Test
    public void testGetScope() {
        final Map<String, Object> scope = gauge.getScope();
        assertThat(scope, is(equalTo(TEST_SCOPE)));
    }

    @Test
    public void testGetResource() {
        final Map<String, Object> resource = gauge.getResource();
        assertThat(resource, is(equalTo(TEST_RESOURCE)));
    }

    @Test
    public void testGetTime() {
        final String endTime = gauge.getTime();
        assertThat(endTime, is(equalTo(TEST_TIME)));
    }

    @Test
    public void testGetUnit() {
        final String unit = gauge.getUnit();
        assertThat(unit, is(equalTo(TEST_UNIT_NAME)));
    }

    @Test
    public void testGetValue() {
        final Double value = gauge.getValue();
        assertThat(value, is(equalTo(TEST_VALUE)));
    }

    @Test
    public void testGetExemplars() {
        List<? extends Exemplar> exemplars = gauge.getExemplars();
        assertThat(exemplars.size(), is(equalTo(2)));
        Exemplar e1 = exemplars.get(0);
        Exemplar e2 = exemplars.get(1);

        assertThat(e1.getTime(), equalTo("1970-01-01T00:00:00Z"));
        assertThat(e1.getValue(), equalTo(2.0));
        assertThat(e1.getSpanId(), equalTo("xsdf"));
        assertThat(e1.getTraceId(), equalTo("abcd"));

        assertThat(e2.getTime(), equalTo("1971-01-01T00:00:00Z"));
        assertThat(e2.getValue(), equalTo(5.0));
        assertThat(e2.getSpanId(), equalTo("xsdt"));
        assertThat(e2.getTraceId(), equalTo("asdf"));
    }

    @Test
    public void testBuilder_missingNonNullParameters_throwsNullPointerException() {
        final JacksonGauge.Builder builder = JacksonGauge.builder();
        builder.withValue(null);
        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    public void testBuilder_withEmptyTime_throwsIllegalArgumentException() {
        builder.withTime("");
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    public void testGaugeToJsonString() throws Exception {
        gauge.put("foo", "bar");
        final String value = UUID.randomUUID().toString();
        gauge.put("testObject", new TestObject(value));
        gauge.put("list", Arrays.asList(1, 4, 5));
        final String result = gauge.toJsonString();

        String file = IOUtils.toString(this.getClass().getResourceAsStream("/testjson/gauge.json"));
        String expected = String.format(file, value, TEST_TIME_KEY1, TEST_KEY2);
        JSONAssert.assertEquals(expected, result, false);
        String attrKey = UUID.randomUUID().toString();
        String attrVal = UUID.randomUUID().toString();
        final Map<String, Object> attributes = Map.of(attrKey, attrVal);
        gauge.put("attributes", attributes);
        final String resultAttr = gauge.toJsonString();
        String attrString = String.format("\"attributes\":{\"%s\":\"%s\"}", attrKey, attrVal);
        assertThat(resultAttr.indexOf(attrString), equalTo(-1));
    }

    @Test
    public void testGaugeToJsonStringWithAttributes() throws JSONException {
        gauge = builder.build(false);
        gauge.put("foo", "bar");
        final String value = UUID.randomUUID().toString();
        gauge.put("testObject", new TestObject(value));
        gauge.put("list", Arrays.asList(1, 4, 5));

        String attrKey = UUID.randomUUID().toString();
        String attrVal = UUID.randomUUID().toString();
        final Map<String, Object> attributes = Map.of(attrKey, attrVal);
        gauge.put("attributes", attributes);
        final String resultAttr = gauge.toJsonString();
        String attrString = String.format("\"attributes\":{\"%s\":\"%s\"}", attrKey, attrVal);
        assertThat(resultAttr.indexOf(attrString), not(equalTo(-1)));

    }

    @Test
    public void testGetSchemaUrl() {
        final String url = gauge.getSchemaUrl();
        assertThat(url, is(equalTo(TEST_SCHEMA_URL)));
    }

    @Test
    public void testGetFlags() {
        final Integer flags = gauge.getFlags();
        assertThat(flags, is(equalTo(TEST_FLAGS)));
    }
}
