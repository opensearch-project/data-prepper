/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.common.sink;

public interface SinkMetrics {
    public void incrementEventsSuccessCounter(int value);
    public void incrementRequestsSuccessCounter(int value);
    public void incrementEventsFailedCounter(int value);
    public void incrementRequestsFailedCounter(int value);
    public void incrementEventsDroppedCounter(int value);
    public void incrementRetries(int value);
    public void recordRequestLatency(double value);
    public void recordRequestSize(double value);
}
