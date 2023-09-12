/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.worker;

import org.hamcrest.Matchers;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.opensearch.dataprepper.plugins.source.opensearch.worker.WorkerCommonUtils.MAX_BACKOFF;

public class WorkerCommonUtilsTest {

    @ParameterizedTest
    @MethodSource("retryCountToExpectedBackoffRange")
    void calculateLinearBackoffAndJitter_returns_expected_backoff_range_for_retryCount(
            final int retryCount, final long minExpectedBackoff, final long maxExpectedBackoff) {

        final long backOffForRetryCount = WorkerCommonUtils.calculateExponentialBackoffAndJitter(retryCount);

        assertThat(backOffForRetryCount, Matchers.greaterThanOrEqualTo(minExpectedBackoff));
        assertThat(backOffForRetryCount, Matchers.lessThanOrEqualTo(maxExpectedBackoff));
    }

    private static Stream<Arguments> retryCountToExpectedBackoffRange() {
        return Stream.of(
                Arguments.of(1, 1, 2_500),
                Arguments.of(2, 1, 3_000),
                Arguments.of(3, 1, 4_000),
                Arguments.of(4, 2_000, 6_000),
                Arguments.of(7, 30_000, 34_000),
                Arguments.of(8, MAX_BACKOFF.toMillis(), MAX_BACKOFF.toMillis())
        );
    }

}
