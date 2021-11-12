/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.model.trace;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
    public void testBuilder_withAllParameters_createsTraceGroupFields() {

        final DefaultTraceGroupFields result = DefaultTraceGroupFields.builder()
                .withDurationInNanos(TEST_DURATION)
                .withStatusCode(TEST_STATUS_CODE)
                .withEndTime(TEST_END_TIME)
                .build();

        assertThat(result, is(notNullValue()));
    }

    @Test
    public void testBuilder_withoutParameters_throws_nullPointerException() {
        final DefaultTraceGroupFields.Builder builder = DefaultTraceGroupFields.builder();
        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    public void testBuilder_throwsNullPointerException_whenEndTimeIsMissing() {

        final DefaultTraceGroupFields.Builder builder = DefaultTraceGroupFields.builder()
                .withDurationInNanos(TEST_DURATION)
                .withStatusCode(TEST_STATUS_CODE);

        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    public void testBuilder_throwsIllegalArgumentException_whenEndTimeIsEmptyString() {

        final DefaultTraceGroupFields.Builder builder = DefaultTraceGroupFields.builder()
                .withDurationInNanos(TEST_DURATION)
                .withStatusCode(TEST_STATUS_CODE)
                .withEndTime("");

        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    public void testBuilder_throwsNullPointerException_whenStatusCodeIsMissing() {

        final DefaultTraceGroupFields.Builder builder = DefaultTraceGroupFields.builder()
                .withDurationInNanos(123L)
                .withEndTime(TEST_END_TIME);

        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    public void testBuilder_throwsNullPointerException_whenDurationIsMissing() {

        final DefaultTraceGroupFields.Builder builder = DefaultTraceGroupFields.builder()
                .withStatusCode(200)
                .withEndTime(TEST_END_TIME);

        assertThrows(NullPointerException.class, builder::build);
    }
}
