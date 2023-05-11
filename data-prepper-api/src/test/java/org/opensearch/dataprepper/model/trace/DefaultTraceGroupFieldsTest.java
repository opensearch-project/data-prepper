/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.trace;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.TestObject;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class DefaultTraceGroupFieldsTest {

    private static final String TEST_END_TIME = UUID.randomUUID().toString();
    private static final Long TEST_DURATION = 123L;
    private static final Integer TEST_STATUS_CODE = 200;

    private DefaultTraceGroupFields defaultTraceGroupFields;

    @BeforeEach
    public void setup() {
        defaultTraceGroupFields = DefaultTraceGroupFields.builder()
                .withDurationInNanos(TEST_DURATION)
                .withStatusCode(TEST_STATUS_CODE)
                .withEndTime(TEST_END_TIME)
                .build();
    }

    @Test
    public void testGetDurationInNanos() {
        final Long durationInNanos = defaultTraceGroupFields.getDurationInNanos();

        assertThat(durationInNanos, is(TEST_DURATION));
    }

    @Test
    public void testGetStatusCode() {
        final Integer statusCode = defaultTraceGroupFields.getStatusCode();

        assertThat(statusCode, is(TEST_STATUS_CODE));
    }

    @Test
    public void testGetEndTime() {
        final String endTime = defaultTraceGroupFields.getEndTime();

        assertThat(endTime, is(TEST_END_TIME));
    }

    @Test
    public void testEquals_withDifferentObject() {
        assertThat(defaultTraceGroupFields, is(not(equalTo(new TestObject()))));
    }

    @Test
    public void testEquals_withDifferentDurationInNanos() {
        final DefaultTraceGroupFields traceGroupFields = DefaultTraceGroupFields.builder()
                .withDurationInNanos(TEST_DURATION - 1)
                .withStatusCode(TEST_STATUS_CODE)
                .withEndTime(TEST_END_TIME)
                .build();
        assertThat(defaultTraceGroupFields, is(not(equalTo(traceGroupFields))));
    }

    @Test
    public void testEquals_withDifferentStatusCode() {
        final DefaultTraceGroupFields traceGroupFields = DefaultTraceGroupFields.builder()
                .withDurationInNanos(TEST_DURATION)
                .withStatusCode(404)
                .withEndTime(TEST_END_TIME)
                .build();
        assertThat(defaultTraceGroupFields, is(not(equalTo(traceGroupFields))));
    }

    @Test
    public void testEquals_withDifferentEndTime() {
        final DefaultTraceGroupFields traceGroupFields = DefaultTraceGroupFields.builder()
                .withDurationInNanos(TEST_DURATION)
                .withStatusCode(TEST_STATUS_CODE)
                .withEndTime("Different from TEST_END_TIME")
                .build();
        assertThat(defaultTraceGroupFields, is(not(equalTo(traceGroupFields))));
    }

    @Test
    public void testEquals() {
        final DefaultTraceGroupFields traceGroupFields = DefaultTraceGroupFields.builder()
                .withDurationInNanos(TEST_DURATION)
                .withStatusCode(TEST_STATUS_CODE)
                .withEndTime(TEST_END_TIME)
                .build();
        assertThat(defaultTraceGroupFields, is(equalTo(traceGroupFields)));
    }

    @Test
    public void testBuilder_withAllParameters_createsTraceGroupFields() {

        final DefaultTraceGroupFields result = DefaultTraceGroupFields.builder()
                .withDurationInNanos(TEST_DURATION)
                .withStatusCode(TEST_STATUS_CODE)
                .withEndTime(TEST_END_TIME)
                .build();

        assertThat(result, is(notNullValue()));
    }
}
