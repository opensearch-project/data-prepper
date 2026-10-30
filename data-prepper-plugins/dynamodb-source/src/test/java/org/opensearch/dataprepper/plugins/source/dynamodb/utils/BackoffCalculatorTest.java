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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.dataprepper.plugins.source.dynamodb.utils.BackoffCalculator.DEFAULT_NEW_SHARD_ACQUISITION_BACKOFF_MAX;
import static org.opensearch.dataprepper.plugins.source.dynamodb.utils.BackoffCalculator.DEFAULT_SHARD_DISTRIBUTION_BACKOFF_MAX;

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
                Arguments.of(8, 2, DEFAULT_NEW_SHARD_ACQUISITION_BACKOFF_MAX.toMillis(), DEFAULT_NEW_SHARD_ACQUISITION_BACKOFF_MAX.toMillis()),
                Arguments.of(70, 2, DEFAULT_NEW_SHARD_ACQUISITION_BACKOFF_MAX.toMillis(), DEFAULT_NEW_SHARD_ACQUISITION_BACKOFF_MAX.toMillis())
        );
    }

    @ParameterizedTest
    @MethodSource("countsToExpectedBackoffRangeWithConfiguredMax")
    void calculateBackoffToAcquireNextShardRespectsConfiguredNewShardAcquisitionBackoffMax(
            final int noAvailableShardCount,
            final Duration configuredMaxBackoff,
            final long minExpectedBackoff,
            final long maxExpectedBackoff
    ) {
        final BackoffCalculator objectUnderTest = new BackoffCalculator(false, configuredMaxBackoff, DEFAULT_SHARD_DISTRIBUTION_BACKOFF_MAX);

        final long backOffForShardCounts = objectUnderTest.calculateBackoffToAcquireNextShard(noAvailableShardCount, new AtomicInteger(0));

        assertThat(backOffForShardCounts, Matchers.greaterThanOrEqualTo(minExpectedBackoff));
        assertThat(backOffForShardCounts, Matchers.lessThanOrEqualTo(maxExpectedBackoff));
    }

    private static Stream<Arguments> countsToExpectedBackoffRangeWithConfiguredMax() {
        return Stream.of(
                Arguments.of(1, Duration.ofSeconds(2), 1, 2_000),
                Arguments.of(2, Duration.ofSeconds(5), 1, 3_000),
                Arguments.of(3, Duration.ofSeconds(2), 1, 2_000),
                Arguments.of(8, Duration.ofSeconds(2), 2_000, 2_000),
                Arguments.of(70, Duration.ofSeconds(2), 2_000, 2_000),
                Arguments.of(70, Duration.ofSeconds(5), 5_000, 5_000),
                Arguments.of(7, Duration.ofSeconds(60), 30_000, 34_000),
                Arguments.of(70, Duration.ofSeconds(60), 60_000, 60_000)
        );
    }

    @ParameterizedTest
    @MethodSource("shardsAcquiredToExpectedBackoffWithConfiguredMax")
    void calculateBackoffToAcquireNextShardWithShardsAcquiredRespectsConfiguredShardDistributionBackoffMax(
            final int shardsAcquiredCount,
            final Duration configuredMaxBackoff,
            final long expectedBackoff
    ) {
        final BackoffCalculator objectUnderTest = new BackoffCalculator(false, DEFAULT_NEW_SHARD_ACQUISITION_BACKOFF_MAX, configuredMaxBackoff);

        final long backOffForShardCounts = objectUnderTest.calculateBackoffToAcquireNextShard(0, new AtomicInteger(shardsAcquiredCount));

        assertThat(backOffForShardCounts, Matchers.equalTo(expectedBackoff));
    }

    private static Stream<Arguments> shardsAcquiredToExpectedBackoffWithConfiguredMax() {
        return Stream.of(
                Arguments.of(0, Duration.ofSeconds(2), 500),
                Arguments.of(3, Duration.ofSeconds(2), 1_500),
                Arguments.of(4, Duration.ofSeconds(2), 2_000),
                Arguments.of(5, Duration.ofSeconds(2), 2_000),
                Arguments.of(30, Duration.ofSeconds(2), 2_000),
                Arguments.of(30, Duration.ofSeconds(5), 5_000),
                Arguments.of(60, Duration.ofSeconds(60), 30_000),
                Arguments.of(200, Duration.ofSeconds(60), 60_000)
        );
    }

    @Test
    void newShardAcquisitionBackoffMaxDoesNotAffectBackoffWithShardsAcquired() {
        final BackoffCalculator objectUnderTest = new BackoffCalculator(false, Duration.ofSeconds(2), Duration.ofSeconds(60));

        final long backOffWithShards = objectUnderTest.calculateBackoffToAcquireNextShard(0, new AtomicInteger(200));

        assertThat(backOffWithShards, Matchers.equalTo(60_000L));
    }

    @Test
    void shardDistributionBackoffMaxDoesNotAffectBackoffWithNoShardsAvailable() {
        final BackoffCalculator objectUnderTest = new BackoffCalculator(false, Duration.ofSeconds(60), Duration.ofSeconds(2));

        final long backOffNoShards = objectUnderTest.calculateBackoffToAcquireNextShard(70, new AtomicInteger(0));

        assertThat(backOffNoShards, Matchers.equalTo(60_000L));
    }

    @Test
    void constructorRejectsNullNewShardAcquisitionBackoffMax() {
        assertThrows(NullPointerException.class, () -> new BackoffCalculator(false, null, DEFAULT_SHARD_DISTRIBUTION_BACKOFF_MAX));
    }

    @Test
    void constructorRejectsNullShardDistributionBackoffMax() {
        assertThrows(NullPointerException.class, () -> new BackoffCalculator(false, DEFAULT_NEW_SHARD_ACQUISITION_BACKOFF_MAX, null));
    }

    @Test
    void defaultConstructorUsesDefaultMaxBackoffs() {
        final BackoffCalculator objectUnderTest = new BackoffCalculator(false);

        final long backOffNoShards = objectUnderTest.calculateBackoffToAcquireNextShard(70, new AtomicInteger(0));
        final long backOffWithShards = objectUnderTest.calculateBackoffToAcquireNextShard(0, new AtomicInteger(200));

        assertThat(backOffNoShards, Matchers.equalTo(DEFAULT_NEW_SHARD_ACQUISITION_BACKOFF_MAX.toMillis()));
        assertThat(backOffWithShards, Matchers.equalTo(DEFAULT_SHARD_DISTRIBUTION_BACKOFF_MAX.toMillis()));
        assertThat(DEFAULT_NEW_SHARD_ACQUISITION_BACKOFF_MAX, Matchers.equalTo(StreamConfig.DEFAULT_NEW_SHARD_ACQUISITION_BACKOFF_MAX));
        assertThat(DEFAULT_SHARD_DISTRIBUTION_BACKOFF_MAX, Matchers.equalTo(StreamConfig.DEFAULT_SHARD_DISTRIBUTION_BACKOFF_MAX));
    }
}
