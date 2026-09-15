/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class AdaptiveBackoffStateTest {

    private static final long INITIAL_DELAY_MS = 50;
    private static final int DECAY_THRESHOLD = 3;

    private AdaptiveBackoffState state;

    @BeforeEach
    void setUp() {
        state = new AdaptiveBackoffState(INITIAL_DELAY_MS, DECAY_THRESHOLD);
    }

    @Test
    void getStartingDelay_returns_initial_delay_when_no_retries_recorded() {
        assertThat(state.getStartingDelay(), equalTo(INITIAL_DELAY_MS));
    }

    @Test
    void recordRetrySuccess_updates_starting_delay() {
        state.recordRetrySuccess(6400);
        assertThat(state.getStartingDelay(), equalTo(6400L));
    }

    @Test
    void recordRetrySuccess_resets_consecutive_success_counter() {
        state.recordFirstAttemptSuccess();
        state.recordFirstAttemptSuccess();
        state.recordRetrySuccess(3200);
        // Next 3 successes should decay from 3200, not trigger earlier
        state.recordFirstAttemptSuccess();
        state.recordFirstAttemptSuccess();
        assertThat(state.getStartingDelay(), equalTo(3200L));
        state.recordFirstAttemptSuccess();
        assertThat(state.getStartingDelay(), equalTo(1600L));
    }

    @Test
    void recordFirstAttemptSuccess_decays_after_threshold_consecutive_successes() {
        state.recordRetrySuccess(6400);

        state.recordFirstAttemptSuccess();
        state.recordFirstAttemptSuccess();
        assertThat(state.getStartingDelay(), equalTo(6400L));

        state.recordFirstAttemptSuccess(); // 3rd = threshold
        assertThat(state.getStartingDelay(), equalTo(3200L));
    }

    @Test
    void decay_continues_halving_on_each_threshold() {
        state.recordRetrySuccess(6400);

        // First decay: 6400 -> 3200
        for (int i = 0; i < DECAY_THRESHOLD; i++) state.recordFirstAttemptSuccess();
        assertThat(state.getStartingDelay(), equalTo(3200L));

        // Second decay: 3200 -> 1600
        for (int i = 0; i < DECAY_THRESHOLD; i++) state.recordFirstAttemptSuccess();
        assertThat(state.getStartingDelay(), equalTo(1600L));

        // Third decay: 1600 -> 800
        for (int i = 0; i < DECAY_THRESHOLD; i++) state.recordFirstAttemptSuccess();
        assertThat(state.getStartingDelay(), equalTo(800L));
    }

    @Test
    void decay_floors_at_initial_delay() {
        state.recordRetrySuccess(100);

        for (int i = 0; i < DECAY_THRESHOLD; i++) state.recordFirstAttemptSuccess();
        // 100 / 2 = 50 = INITIAL_DELAY_MS (floor)
        assertThat(state.getStartingDelay(), equalTo(INITIAL_DELAY_MS));

        // Should not go below floor
        for (int i = 0; i < DECAY_THRESHOLD; i++) state.recordFirstAttemptSuccess();
        assertThat(state.getStartingDelay(), equalTo(INITIAL_DELAY_MS));
    }

    @Test
    void concurrent_access_does_not_throw() throws InterruptedException {
        Thread t1 = new Thread(() -> {
            for (int i = 0; i < 1000; i++) {
                state.recordRetrySuccess(i * 100L);
                state.getStartingDelay();
            }
        });
        Thread t2 = new Thread(() -> {
            for (int i = 0; i < 1000; i++) {
                state.recordFirstAttemptSuccess();
                state.getStartingDelay();
            }
        });
        t1.start();
        t2.start();
        t1.join();
        t2.join();
        // No exception = pass. Value is non-deterministic due to concurrency.
        assertThat(state.getStartingDelay() >= INITIAL_DELAY_MS, equalTo(true));
    }
}
