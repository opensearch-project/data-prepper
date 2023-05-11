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
import org.skyscreamer.jsonassert.JSONAssert;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JacksonGaugeTest {

    private static final String TEST_KEY2 = UUID.randomUUID().toString();
    private static final Long TEST_TIME_KEY1 = new Date().getTime();
    private static final Map<String, Object> TEST_ATTRIBUTES = ImmutableMap.of(
            "key1", TEST_TIME_KEY1,
            "key2", TEST_KEY2);
    private static final String TEST_SERVICE_NAME = "service";
    private static final String TEST_NAME = "name";
    private static final String TEST_DESCRIPTION = "description";
    private static final String TEST_UNIT_NAME = "unit";
    private static final String TEST_START_TIME = "2022-01-01T00:00:00Z";
    private static final String TEST_TIME = "2022-01-02T00:00:00Z";
    private static final String TEST_EVENT_KIND = Metric.KIND.GAUGE.name();
    private static final Double TEST_VALUE = 1D;
    private static final String TEST_SCHEMA_URL = "schema";
    private static final Integer TEST_FLAGS = 1;

    private static final List<Exemplar> TEST_EXEMPLARS = Arrays.asList(
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
    public void testGaugeToJsonString() throws JSONException {
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
