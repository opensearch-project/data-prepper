/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import java.util.function.ToDoubleFunction;

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
    public static final String CLOUDWATCH_LOGS_REQUEST_MULTI_FAILED = "cloudWatchLogsRequestMultipleFailures";
    public static final String CLOUDWATCH_LOGS_LARGE_EVENTS_DROPPED = "cloudWatchLogsLargeEventsDropped";
    public static final String CLOUDWATCH_LOGS_LOG_SIZE = "cloudWatchLogsLogSize";
    public static final String CLOUDWATCH_LOGS_REQUEST_SIZE = "cloudWatchLogsRequestSize";
    public static final String CLOUDWATCH_LOGS_ENTITY_REJECTED = "cloudWatchLogsEntityRejected";
    public static final String CLOUDWATCH_LOGS_UNHANDLED_ERROR = "cloudWatchLogsUnhandledError";
    public static final String CLOUDWATCH_LOGS_ACCESS_DENIED = "cloudWatchLogsAccessDenied";
    public static final String CLOUDWATCH_LOGS_RESOURCE_NOT_FOUND = "cloudWatchLogsResourceNotFound";
    public static final String CLOUDWATCH_LOGS_THROTTLED = "cloudWatchLogsThrottled";
    public static final String CLOUDWATCH_LOGS_ENTITY_GROUPS_CREATED = "cloudWatchLogsEntityGroupsCreated";
    public static final String CLOUDWATCH_LOGS_ENTITY_CARDINALITY = "cloudWatchLogsEntityCardinality";
    public static final String CLOUDWATCH_LOGS_ENTITY_OVERFLOW_EVENTS = "cloudWatchLogsEntityOverflowEvents";
    private final Counter logEventSuccessCounter;
    private final Counter logEventFailCounter;
    private final Counter requestSuccessCount;
    private final Counter requestFailCount;
    private final Counter requestMultiFailCount;
    private final Counter logLargeEventsDroppedCounter;
    private final Counter entityRejectedCounter;
    private final Counter unhandledErrorCounter;
    private final Counter accessDeniedCounter;
    private final Counter resourceNotFoundCounter;
    private final Counter throttledCounter;
    private final Counter entityGroupsCreatedCounter;
    private final Counter entityOverflowEventsCounter;
    private final DistributionSummary logSizeMetric;
    private final DistributionSummary requestSizeMetric;
    private final PluginMetrics pluginMetrics;

    public CloudWatchLogsMetrics(final PluginMetrics pluginMetrics) {
        this.pluginMetrics = pluginMetrics;
        this.logEventSuccessCounter = pluginMetrics.counter(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_EVENTS_SUCCEEDED);
        this.requestFailCount = pluginMetrics.counter(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_REQUESTS_FAILED);
        this.requestMultiFailCount = pluginMetrics.counter(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_REQUEST_MULTI_FAILED);
        this.logEventFailCounter = pluginMetrics.counter(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_EVENTS_FAILED);
        this.requestSuccessCount = pluginMetrics.counter(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_REQUESTS_SUCCEEDED);
        this.logLargeEventsDroppedCounter = pluginMetrics.counter(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_LARGE_EVENTS_DROPPED);
        this.entityRejectedCounter = pluginMetrics.counter(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_ENTITY_REJECTED);
        this.unhandledErrorCounter = pluginMetrics.counter(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_UNHANDLED_ERROR);
        this.accessDeniedCounter = pluginMetrics.counter(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_ACCESS_DENIED);
        this.resourceNotFoundCounter = pluginMetrics.counter(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_RESOURCE_NOT_FOUND);
        this.throttledCounter = pluginMetrics.counter(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_THROTTLED);
        this.entityGroupsCreatedCounter = pluginMetrics.counter(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_ENTITY_GROUPS_CREATED);
        this.entityOverflowEventsCounter = pluginMetrics.counter(CloudWatchLogsMetrics.CLOUDWATCH_LOGS_ENTITY_OVERFLOW_EVENTS);
        this.logSizeMetric = pluginMetrics.summary(CLOUDWATCH_LOGS_LOG_SIZE);
        this.requestSizeMetric = pluginMetrics.summary(CLOUDWATCH_LOGS_REQUEST_SIZE);
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

    public void increaseRequestMultiFailCounter(int value) {
        requestMultiFailCount.increment(value);
    }

    public void increaseLogLargeEventsDroppedCounter(int value) {
        logLargeEventsDroppedCounter.increment(value);
    }

    public void increaseEntityRejectedCounter(int value) {
        entityRejectedCounter.increment(value);
    }

    public void increaseUnhandledErrorCounter(int value) {
        unhandledErrorCounter.increment(value);
    }

    public void increaseAccessDeniedCounter(int value) {
        accessDeniedCounter.increment(value);
    }

    public void increaseResourceNotFoundCounter(int value) {
        resourceNotFoundCounter.increment(value);
    }

    public void increaseThrottledCounter(int value) {
        throttledCounter.increment(value);
    }

    /**
     * Counts entity buffer groups created for a real resolved entity (dynamic-entity mode). The shared
     * overflow group is excluded so this stays a count of genuine entities; see
     * {@link #increaseEntityOverflowEventsCounter(int)} for the overflow bucket.
     */
    public void increaseEntityGroupsCreatedCounter(int value) {
        entityGroupsCreatedCounter.increment(value);
    }

    /**
     * Counts events that could not be given their own entity group because the cardinality bound was
     * already reached, and were buffered in the shared fallback group with no entity instead. Any
     * non-zero value means events are losing their per-resource entity attribution.
     */
    public void increaseEntityOverflowEventsCounter(int value) {
        entityOverflowEventsCounter.increment(value);
    }

    /**
     * Registers a gauge reporting the number of entity buffer groups currently held, i.e. the live
     * entity cardinality. The gauge holds a weak reference to {@code stateObject}, so the caller must
     * retain it for as long as the metric should be reported.
     */
    public <T> void registerEntityCardinalityGauge(final T stateObject, final ToDoubleFunction<T> valueFunction) {
        pluginMetrics.gauge(CLOUDWATCH_LOGS_ENTITY_CARDINALITY, stateObject, valueFunction);
    }

    public void recordLogSize(int value) {
        logSizeMetric.record(value);
    }

    public void recordRequestSize(int value) {
        requestSizeMetric.record(value);
    }
}
