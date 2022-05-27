/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BackoffUtilsTest {
    private static final int NUMBER_OF_RETRIES = 5;
    private static final int TIME_TO_WAIT = 50;

    BackoffUtils backoffUtils;

    @BeforeEach
    void setUp() {
        backoffUtils = new BackoffUtils(NUMBER_OF_RETRIES, TIME_TO_WAIT);
    }


    @Test
    void shouldRetry_should_return_true_for_value_greater_than_zero() {
        assertTrue(backoffUtils.shouldRetry());
    }

    @Test
    void shouldRetry_should_return_false_for_zero() {
        backoffUtils = new BackoffUtils(0, 1000);
        assertFalse(backoffUtils.shouldRetry());
    }

    @Test
    void errorOccurred_should_decrement_retires() {
        backoffUtils.errorOccurred();
        assertThat(backoffUtils.getNumberOfTriesLeft(), equalTo(NUMBER_OF_RETRIES - 1));
    }

    @Test
    void doNotRetry_should_set_retries_to_zero() {
        backoffUtils.doNotRetry();
        assertThat(backoffUtils.getNumberOfTriesLeft(), equalTo(0));
    }
}