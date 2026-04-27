/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.common.sink;

import java.util.concurrent.TimeUnit;

public interface SinkMetrics {
    void incrementEventsSuccessCounter(int value);
    void incrementRequestsSuccessCounter(int value);
    void incrementEventsFailedCounter(int value);
    void incrementRequestsFailedCounter(int value);
    void incrementEventsDroppedCounter(int value);
    void incrementRetries(int value);

    /**
     * Records request latency
     * @param amount Amount of time
     * @param unit Units for amount
     */
    void recordRequestLatency(long amount, TimeUnit unit);
    /**
     * Records request latency as nanos
     * @deprecated Use @{link recordRequestLatency(long, TimeUnit)} instead
     * @param value nanoseconds
     */
    @Deprecated
    void recordRequestLatency(double value);
    void recordRequestSize(double value);
}
