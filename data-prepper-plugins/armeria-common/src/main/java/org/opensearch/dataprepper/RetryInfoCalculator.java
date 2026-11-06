/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper;

import com.google.rpc.RetryInfo;
import org.opensearch.dataprepper.model.retry.RetryDelayCalculator;

import java.time.Duration;

/**
 * Wraps {@link RetryDelayCalculator} and returns the retry delay as a gRPC {@link RetryInfo}.
 */
public class RetryInfoCalculator {

    public static final Duration DEFAULT_MINIMUM_DELAY = RetryDelayCalculator.DEFAULT_MINIMUM_DELAY;
    public static final Duration DEFAULT_MAXIMUM_DELAY = RetryDelayCalculator.DEFAULT_MAXIMUM_DELAY;

    private final RetryDelayCalculator retryDelayCalculator;

    public RetryInfoCalculator(Duration minimumDelay, Duration maximumDelay) {
        this.retryDelayCalculator = new RetryDelayCalculator(minimumDelay, maximumDelay);
    }

    private static RetryInfo createProtoResult(Duration delay) {
        return RetryInfo.newBuilder().setRetryDelay(mapDuration(delay)).build();
    }

    private static com.google.protobuf.Duration.Builder mapDuration(Duration duration) {
        return com.google.protobuf.Duration.newBuilder().setSeconds(duration.getSeconds()).setNanos(duration.getNano());
    }

    public RetryInfo createRetryInfo() {
        return createProtoResult(retryDelayCalculator.nextDelay());
    }

}
