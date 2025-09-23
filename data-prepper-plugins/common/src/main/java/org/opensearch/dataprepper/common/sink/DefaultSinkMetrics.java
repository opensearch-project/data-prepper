/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.common.sink;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.opensearch.dataprepper.metrics.PluginMetrics;

public class DefaultSinkMetrics implements SinkMetrics {
    public static final String DEFAULT_EVENT_NAME = "Event";
    public static final String SINK_REQUESTS_SUCCEEDED = "SinkRequestsSucceeded";
    public static final String SINK_EVENTS_SUCCEEDED = "SinkEventsSucceeded";
    public static final String SINK_EVENTS_FAILED = "SinkEventsFailed";
    public static final String SINK_REQUESTS_FAILED = "SinkRequestsFailed";
    public static final String SINK_REQUEST_LATENCY = "SinkRequestLatency";
    public static final String SINK_REQUEST_SIZE = "SinkRequestSize";
    private final Counter sinkRequestsSucceeded;
    private final Counter sinkEventsSucceeded;
    private final Counter sinkRequestsFailed;
    private final Counter sinkEventsFailed;
    private final DistributionSummary sinkRequestLatency;
    private final DistributionSummary sinkRequestSize;

    public DefaultSinkMetrics(final PluginMetrics pluginMetrics, final String sinkPrefix, final String eventName) {
        this.sinkRequestsSucceeded = pluginMetrics.counter(sinkPrefix + SINK_REQUESTS_SUCCEEDED);
        this.sinkEventsSucceeded = pluginMetrics.counter(sinkPrefix + "Sink"+eventName+"sSucceeded");
        this.sinkRequestsFailed = pluginMetrics.counter(sinkPrefix + SINK_REQUESTS_FAILED);
        this.sinkEventsFailed = pluginMetrics.counter(sinkPrefix + "Sink"+eventName+"sFailed");
        this.sinkRequestLatency = pluginMetrics.summary(sinkPrefix + SINK_REQUEST_LATENCY);
        this.sinkRequestSize = pluginMetrics.summary(sinkPrefix + SINK_REQUEST_SIZE);
    }

    public void incrementEventsSuccessCounter(int value){
        sinkEventsSucceeded.increment(value);
    }

    public void incrementRequestsSuccessCounter(int value){
        sinkRequestsSucceeded.increment(value);
    }

    public void incrementEventsFailedCounter(int value){
        sinkEventsFailed.increment(value);
    }

    public void incrementRequestsFailedCounter(int value){
        sinkRequestsFailed.increment(value);
    }

    public void recordRequestLatency(double value){
        sinkRequestLatency.record(value);
    }

    public void recordRequestSize(double value){
        sinkRequestSize.record(value);
    }

}

