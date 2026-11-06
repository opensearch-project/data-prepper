/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.retry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RetryDelayCalculatorTest {
    private static final Duration MINIMUM_DELAY = Duration.ofSeconds(1);
    private static final Duration MAXIMUM_DELAY = Duration.ofSeconds(4);

    private MutableClock clock;

    @BeforeEach
    void setUp() {
        clock = new MutableClock(Instant.parse("2026-11-06T10:00:00Z"));
    }

    private RetryDelayCalculator createObjectUnderTest() {
        return new RetryDelayCalculator(MINIMUM_DELAY, MAXIMUM_DELAY, clock);
    }

    @Test
    void constructor_throws_if_minimumDelay_is_null() {
        assertThrows(NullPointerException.class, () -> new RetryDelayCalculator(null, MAXIMUM_DELAY));
    }

    @Test
    void constructor_throws_if_maximumDelay_is_null() {
        assertThrows(NullPointerException.class, () -> new RetryDelayCalculator(MINIMUM_DELAY, null));
    }

    @Test
    void nextDelay_returns_minimum_delay_on_first_call() {
        assertThat(createObjectUnderTest().nextDelay(), equalTo(MINIMUM_DELAY));
    }

    @Test
    void nextDelay_doubles_on_consecutive_calls_up_to_maximum_delay() {
        final RetryDelayCalculator objectUnderTest = createObjectUnderTest();

        assertThat(objectUnderTest.nextDelay(), equalTo(Duration.ofSeconds(1)));
        assertThat(objectUnderTest.nextDelay(), equalTo(Duration.ofSeconds(1)));
        assertThat(objectUnderTest.nextDelay(), equalTo(Duration.ofSeconds(2)));
        assertThat(objectUnderTest.nextDelay(), equalTo(Duration.ofSeconds(4)));
        assertThat(objectUnderTest.nextDelay(), equalTo(Duration.ofSeconds(4)));
    }

    @Test
    void nextDelay_keeps_backing_off_while_calls_arrive_within_the_current_delay() {
        final RetryDelayCalculator objectUnderTest = createObjectUnderTest();
        objectUnderTest.nextDelay();
        objectUnderTest.nextDelay();

        clock.advance(Duration.ofSeconds(2));

        assertThat(objectUnderTest.nextDelay(), equalTo(Duration.ofSeconds(2)));
    }

    @Test
    void nextDelay_returns_minimum_delay_again_after_the_current_delay_has_passed() {
        final RetryDelayCalculator objectUnderTest = createObjectUnderTest();
        objectUnderTest.nextDelay();
        objectUnderTest.nextDelay();
        objectUnderTest.nextDelay();

        clock.advance(Duration.ofSeconds(4).plusMillis(1));

        assertThat(objectUnderTest.nextDelay(), equalTo(MINIMUM_DELAY));
        assertThat(objectUnderTest.nextDelay(), equalTo(MINIMUM_DELAY));
        assertThat(objectUnderTest.nextDelay(), equalTo(Duration.ofSeconds(2)));
    }

    @Test
    void nextDelay_does_not_back_off_on_a_quick_first_call_with_a_long_minimum_delay() {
        final RetryDelayCalculator objectUnderTest =
                new RetryDelayCalculator(Duration.ofSeconds(10), Duration.ofSeconds(20), clock);

        assertThat(objectUnderTest.nextDelay(), equalTo(Duration.ofSeconds(10)));
        assertThat(objectUnderTest.nextDelay(), equalTo(Duration.ofSeconds(10)));
    }

    @ParameterizedTest
    @CsvSource({"PT0S,1", "PT0.000000001S,1", "PT0.1S,1", "PT1S,1", "PT1.000000001S,2", "PT1.5S,2", "PT2S,2"})
    void toRetryAfterSeconds_rounds_up_to_whole_seconds_and_is_at_least_one(final Duration delay, final long expectedSeconds) {
        assertThat(RetryDelayCalculator.toRetryAfterSeconds(delay), equalTo(expectedSeconds));
    }

    private static class MutableClock extends Clock {
        private Instant now;

        MutableClock(final Instant start) {
            this.now = start;
        }

        void advance(final Duration duration) {
            now = now.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(final ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}
