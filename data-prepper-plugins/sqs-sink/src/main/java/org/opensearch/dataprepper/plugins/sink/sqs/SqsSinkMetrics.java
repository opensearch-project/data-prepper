/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.sqs;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import java.util.concurrent.TimeUnit;

public class SqsSinkMetrics {
    public static final String SQS_SINK_REQUESTS_SUCCEEDED = "sqsSinkRequestsSucceeded";
    public static final String SQS_SINK_EVENTS_SUCCEEDED = "sqsSinkEventsSucceeded";
    public static final String SQS_SINK_EVENTS_FAILED = "sqsSinkEventsFailed";
    public static final String SQS_SINK_REQUESTS_FAILED = "sqsSinkRequestsFailed";
    public static final String SQS_SINK_REQUEST_LATENCY = "sqsSinkRequestLatency";
    public static final String SQS_SINK_REQUEST_SIZE = "sqsSinkRequestSize";
    private final Counter sqsSinkRequestsSucceeded;
    private final Counter sqsSinkEventsSucceeded;
    private final Counter sqsSinkRequestsFailed;
    private final Counter sqsSinkEventsFailed;
    private final Timer sqsSinkRequestLatency;
    private final DistributionSummary sqsSinkRequestSize;

    public SqsSinkMetrics(final PluginMetrics pluginMetrics) {
        this.sqsSinkRequestsSucceeded = pluginMetrics.counter(SQS_SINK_REQUESTS_SUCCEEDED);
        this.sqsSinkEventsSucceeded = pluginMetrics.counter(SQS_SINK_EVENTS_SUCCEEDED);
        this.sqsSinkRequestsFailed = pluginMetrics.counter(SQS_SINK_REQUESTS_FAILED);
        this.sqsSinkEventsFailed = pluginMetrics.counter(SQS_SINK_EVENTS_FAILED);
        this.sqsSinkRequestLatency = pluginMetrics.timer(SQS_SINK_REQUEST_LATENCY);
        this.sqsSinkRequestSize = pluginMetrics.summary(SQS_SINK_REQUEST_SIZE);
    }

    public void incrementEventsSuccessCounter(int value) {
        sqsSinkEventsSucceeded.increment(value);
    }

    public void incrementRequestsSuccessCounter(int value) {
        sqsSinkRequestsSucceeded.increment(value);
    }

    public void incrementEventsFailedCounter(int value) {
        sqsSinkEventsFailed.increment(value);
    }

    public void incrementRequestsFailedCounter(int value) {
        sqsSinkRequestsFailed.increment(value);
    }

    public void recordRequestLatency(final long amount, final TimeUnit timeUnit) {
        sqsSinkRequestLatency.record(amount, timeUnit);
    }

    public void recordRequestSize(double value) {
        sqsSinkRequestSize.record(value);
    }
}
