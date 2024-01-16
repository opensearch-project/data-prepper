/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.utils;

import org.hamcrest.Matchers;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.opensearch.dataprepper.plugins.source.dynamodb.utils.BackoffCalculator.MAX_BACKOFF_NO_SHARDS_ACQUIRED;

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
                Arguments.of(8, 2, MAX_BACKOFF_NO_SHARDS_ACQUIRED.toMillis(), MAX_BACKOFF_NO_SHARDS_ACQUIRED.toMillis()),
                Arguments.of(70, 2, MAX_BACKOFF_NO_SHARDS_ACQUIRED.toMillis(), MAX_BACKOFF_NO_SHARDS_ACQUIRED.toMillis())
        );
    }
}
