/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.common.sink;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import java.util.concurrent.TimeUnit;

public class DefaultSinkMetrics implements SinkMetrics {
    static final String DEFAULT_EVENT_NAME = "Event";
    static final String SINK_REQUESTS_SUCCEEDED = "sinkRequestsSucceeded";
    static final String SINK_REQUESTS_FAILED = "sinkRequestsFailed";
    static final String SINK_REQUEST_LATENCY = "sinkRequestLatency";
    static final String SINK_RETRIES = "sinkRetries";
    static final String SINK_REQUEST_SIZE = "sinkRequestSize";
    private final Counter sinkRequestsSucceeded;
    private final Counter sinkRequestsFailed;
    private final Counter sinkEventsSucceeded;
    private final Counter sinkEventsFailed;
    private final Counter sinkEventsDropped;
    private final Counter sinkRetries;
    private final Timer sinkRequestLatency;
    private final DistributionSummary sinkRequestSize;
    private final DistributionSummary sinkEventSize;

    public DefaultSinkMetrics(final PluginMetrics pluginMetrics, final String eventName) {
        this.sinkRequestsSucceeded = pluginMetrics.counter(SINK_REQUESTS_SUCCEEDED);
        this.sinkEventsSucceeded = pluginMetrics.counter("sink"+eventName+"sSucceeded");
        this.sinkRequestsFailed = pluginMetrics.counter(SINK_REQUESTS_FAILED);
        this.sinkEventsFailed = pluginMetrics.counter("sink"+eventName+"sFailed");
        this.sinkEventsDropped = pluginMetrics.counter("sink"+eventName+"sDropped");
        this.sinkRetries = pluginMetrics.counter(SINK_RETRIES);
        this.sinkRequestLatency = pluginMetrics.timer(SINK_REQUEST_LATENCY);
        this.sinkRequestSize = pluginMetrics.summary(SINK_REQUEST_SIZE);
        this.sinkEventSize = pluginMetrics.summary("sink"+eventName+"Size");
    }

    public DefaultSinkMetrics(final PluginMetrics pluginMetrics) {
        this(pluginMetrics, DEFAULT_EVENT_NAME);
    }

    public void incrementEventsSuccessCounter(int value){
        sinkEventsSucceeded.increment(value);
    }

    public void incrementRequestsSuccessCounter(int value){
        sinkRequestsSucceeded.increment(value);
    }

    public void incrementEventsFailedCounter(int value) {
        sinkEventsFailed.increment(value);
    }

    public void incrementEventsDroppedCounter(int value) {
        sinkEventsDropped.increment(value);
    }

    public void incrementRequestsFailedCounter(int value) {
        sinkRequestsFailed.increment(value);
    }

    public void incrementRetries(int value) {
        sinkRetries.increment(value);
    }

    @Override
    public void recordRequestLatency(final long amount, final TimeUnit unit) {
        sinkRequestLatency.record(amount, unit);
    }

    public void recordRequestLatency(double value) {
        recordRequestLatency((long)value, TimeUnit.NANOSECONDS);
    }

    public void recordRequestSize(double value){
        sinkRequestSize.record(value);
    }

    public void recordEventSize(double value){
        sinkEventSize.record(value);
    }
}

