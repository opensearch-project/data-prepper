/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.utils;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.dataprepper.plugins.source.dynamodb.configuration.StreamConfig;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.opensearch.dataprepper.plugins.source.dynamodb.utils.BackoffCalculator.DEFAULT_MAX_BACKOFF;

public class BackoffCalculatorTest {

    @ParameterizedTest
    @MethodSource("countsToExpectedBackoffRange")
    void calculateBackoffToAcquireNextShardReturnsExpectedBackoffValues(
            final int noAvailableShardCount,
            final int shardsAcquiredCount,
            final long minExpectedBackoff,
            final long maxExpectedBackoff
    ) {

        final BackoffCalculator objectUnderTest = new BackoffCalculator(false);

        final long backOffForShardCounts = objectUnderTest.calculateBackoffToAcquireNextShard(noAvailableShardCount, new AtomicInteger(shardsAcquiredCount));

        assertThat(backOffForShardCounts, Matchers.greaterThanOrEqualTo(minExpectedBackoff));
        assertThat(backOffForShardCounts, Matchers.lessThanOrEqualTo(maxExpectedBackoff));
    }

    private static Stream<Arguments> countsToExpectedBackoffRange() {
        return Stream.of(
                Arguments.of(0, 0, 500, 500),
                Arguments.of(0, 1, 500, 500),
                Arguments.of(0, 2, 1000, 1000),
                Arguments.of(0, 29, 14_500, 14_500),
                Arguments.of(0, 30, 15_000, 15_000),
                Arguments.of(2, 1, 1, 3_000),
                Arguments.of(3, 0, 1, 4_000),
                Arguments.of(4, 2, 2_000, 6_000),
                Arguments.of(5, 6, 6_000, 10_000),
                Arguments.of(6, 6, 14_000, 15_000),
                Arguments.of(8, 2, DEFAULT_MAX_BACKOFF.toMillis(), DEFAULT_MAX_BACKOFF.toMillis()),
                Arguments.of(70, 2, DEFAULT_MAX_BACKOFF.toMillis(), DEFAULT_MAX_BACKOFF.toMillis())
        );
    }

    @ParameterizedTest
    @MethodSource("countsToExpectedBackoffRangeWithConfiguredMax")
    void calculateBackoffToAcquireNextShardRespectsConfiguredMaxBackoff(
            final int noAvailableShardCount,
            final Duration configuredMaxBackoff,
            final long minExpectedBackoff,
            final long maxExpectedBackoff
    ) {
        final BackoffCalculator objectUnderTest = new BackoffCalculator(false, configuredMaxBackoff);

        final long backOffForShardCounts = objectUnderTest.calculateBackoffToAcquireNextShard(noAvailableShardCount, new AtomicInteger(0));

        assertThat(backOffForShardCounts, Matchers.greaterThanOrEqualTo(minExpectedBackoff));
        assertThat(backOffForShardCounts, Matchers.lessThanOrEqualTo(maxExpectedBackoff));
    }

    private static Stream<Arguments> countsToExpectedBackoffRangeWithConfiguredMax() {
        return Stream.of(
                // below the ceiling the exponential ladder (with +/-2s jitter) is unchanged
                Arguments.of(1, Duration.ofSeconds(2), 1, 2_000),
                Arguments.of(2, Duration.ofSeconds(5), 1, 3_000),
                Arguments.of(3, Duration.ofSeconds(2), 1, 2_000),
                // once saturated the configured ceiling, not the 15s default, bounds the backoff
                Arguments.of(8, Duration.ofSeconds(2), 2_000, 2_000),
                Arguments.of(70, Duration.ofSeconds(2), 2_000, 2_000),
                Arguments.of(70, Duration.ofSeconds(5), 5_000, 5_000),
                // a ceiling above the default lets the ladder grow past 15s
                Arguments.of(7, Duration.ofSeconds(60), 30_000, 34_000),
                Arguments.of(70, Duration.ofSeconds(60), 60_000, 60_000)
        );
    }

    @ParameterizedTest
    @MethodSource("shardsAcquiredToExpectedBackoffWithConfiguredMax")
    void calculateBackoffToAcquireNextShardWithShardsAcquiredRespectsConfiguredMaxBackoff(
            final int shardsAcquiredCount,
            final Duration configuredMaxBackoff,
            final long expectedBackoff
    ) {
        final BackoffCalculator objectUnderTest = new BackoffCalculator(false, configuredMaxBackoff);

        final long backOffForShardCounts = objectUnderTest.calculateBackoffToAcquireNextShard(0, new AtomicInteger(shardsAcquiredCount));

        assertThat(backOffForShardCounts, Matchers.equalTo(expectedBackoff));
    }

    private static Stream<Arguments> shardsAcquiredToExpectedBackoffWithConfiguredMax() {
        return Stream.of(
                // below the ceiling the linear ramp of 500ms per held shard is unchanged
                Arguments.of(0, Duration.ofSeconds(2), 500),
                Arguments.of(3, Duration.ofSeconds(2), 1_500),
                Arguments.of(4, Duration.ofSeconds(2), 2_000),
                // once saturated the configured ceiling, not the 15s default, bounds the backoff
                Arguments.of(5, Duration.ofSeconds(2), 2_000),
                Arguments.of(30, Duration.ofSeconds(2), 2_000),
                Arguments.of(30, Duration.ofSeconds(5), 5_000),
                // a ceiling above the default lets the ramp grow past 15s
                Arguments.of(60, Duration.ofSeconds(60), 30_000),
                Arguments.of(200, Duration.ofSeconds(60), 60_000)
        );
    }

    @Test
    void defaultConstructorUsesDefaultMaxBackoff() {
        final BackoffCalculator objectUnderTest = new BackoffCalculator(false);

        final long backOffNoShards = objectUnderTest.calculateBackoffToAcquireNextShard(70, new AtomicInteger(0));
        final long backOffWithShards = objectUnderTest.calculateBackoffToAcquireNextShard(0, new AtomicInteger(200));

        assertThat(backOffNoShards, Matchers.equalTo(DEFAULT_MAX_BACKOFF.toMillis()));
        assertThat(backOffWithShards, Matchers.equalTo(DEFAULT_MAX_BACKOFF.toMillis()));
        assertThat(DEFAULT_MAX_BACKOFF, Matchers.equalTo(StreamConfig.DEFAULT_SHARD_ACQUISITION_BACKOFF_MAX));
    }
}
