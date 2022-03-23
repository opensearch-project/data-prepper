/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.model.metric;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JacksonGaugeTest {

    private static final Map<String, Object> TEST_ATTRIBUTES = ImmutableMap.of(
            "key1", new Date().getTime(),
            "key2", UUID.randomUUID().toString());
    private static final String TEST_SERVICE_NAME = "service";
    private static final String TEST_NAME = "name";
    private static final String TEST_DESCRIPTION = "description";
    private static final String TEST_UNIT_NAME = "unit";
    private static final String TEST_START_TIME = UUID.randomUUID().toString();
    private static final String TEST_TIME = UUID.randomUUID().toString();
    private static final String TEST_EVENT_KIND = Metric.KIND.GAUGE.name();
    private static final Double TEST_VALUE = 1D;

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
                .withServiceName(TEST_SERVICE_NAME);

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
}