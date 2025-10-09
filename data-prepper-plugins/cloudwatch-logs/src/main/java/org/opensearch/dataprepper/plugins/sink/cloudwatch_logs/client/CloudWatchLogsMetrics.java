/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;

/**
 * Class is meant to abstract the metric book-keeping of
 * CloudWatchLogs metrics so that multiple instances
 * may refer to it.
 */
public class CloudWatchLogsMetrics {
    public static final String CLOUDWATCH_LOGS_REQUESTS_SUCCEEDED = "cloudWatchLogsRequestsSucceeded";
    public static final String CLOUDWATCH_LOGS_EVENTS_SUCCEEDED = "cloudWatchLogsEventsSucceeded";
    public static final String CLOUDWATCH_LOGS_EVENTS_FAILED = "cloudWatchLogsEventsFailed";
    public static final String CLOUDWATCH_LOGS_REQUESTS_FAILED = "cloudWatchLogsRequestsFailed";
    public static final String CLOUDWATCH_LOGS_LARGE_EVENTS_DROPPED = "cloudWatchLogsLargeEventsDropped";
    private final Counter logEventSuccessCounter;
    private final Counter logEventFailCounter;
    private final Counter requestSuccessCount;
    private final Counter requestFailCount;
    private final Counter logLargeEventsDroppedCounter;

    public CloudWatchLogsMetrics(final PluginMetrics pluginMetrics) {
        this.logEventSuccessCounter = pluginMetrics.counter(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_EVENTS_SUCCEEDED);
        this.requestFailCount = pluginMetrics.counter(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_REQUESTS_FAILED);
        this.logEventFailCounter = pluginMetrics.counter(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_EVENTS_FAILED);
        this.requestSuccessCount = pluginMetrics.counter(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_REQUESTS_SUCCEEDED);
        this.logLargeEventsDroppedCounter = pluginMetrics.counter(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_LARGE_EVENTS_DROPPED);
    }

    public void increaseLogEventSuccessCounter(int value) {
        logEventSuccessCounter.increment(value);
    }

    public void increaseRequestSuccessCounter(int value) {
        requestSuccessCount.increment(value);
    }

    public void increaseLogEventFailCounter(int value) {
        logEventFailCounter.increment(value);
    }

    public void increaseRequestFailCounter(int value) {
        requestFailCount.increment(value);
    }

    public void increaseLogLargeEventsDroppedCounter(int value) {
        logLargeEventsDroppedCounter.increment(value);
    }
}
