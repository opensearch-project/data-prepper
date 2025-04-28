/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.sqs;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;

public class SqsSinkMetrics {
    public static final String SQS_SINK_REQUESTS_SUCCEEDED = "sqsSinkRequestsSucceeded";
    public static final String SQS_SINK_EVENTS_SUCCEEDED = "sqsSinkEventsSucceeded";
    public static final String SQS_SINK_EVENTS_FAILED = "sqsSinkEventsFailed";
    public static final String SQS_SINK_REQUESTS_FAILED = "sqsSinkRequestsFailed";
    private final Counter sqsSinkRequestsSucceeded;
    private final Counter sqsSinkEventsSucceeded;
    private final Counter sqsSinkRequestsFailed;
    private final Counter sqsSinkEventsFailed;

    public SqsSinkMetrics(final PluginMetrics pluginMetrics) {
        this.sqsSinkRequestsSucceeded = pluginMetrics.counter(SQS_SINK_REQUESTS_SUCCEEDED);
        this.sqsSinkEventsSucceeded = pluginMetrics.counter(SQS_SINK_EVENTS_SUCCEEDED);
        this.sqsSinkRequestsFailed = pluginMetrics.counter(SQS_SINK_REQUESTS_FAILED);
        this.sqsSinkEventsFailed = pluginMetrics.counter(SQS_SINK_EVENTS_FAILED);
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
}
