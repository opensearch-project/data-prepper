/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
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
    public static final String CLOUDWATCH_LOGS_ENTITY_DROPPED = "cloudWatchLogsEntityDropped";
    public static final String CLOUDWATCH_LOGS_EVENTS_RELEASED_WITHOUT_DELIVERY = "cloudWatchLogsEventsReleasedWithoutDelivery";
    public static final String CLOUDWATCH_LOGS_EVENTS_NEGATIVELY_ACKED = "cloudWatchLogsEventsNegativelyAcked";
    public static final String CLOUDWATCH_LOGS_EVENTS_SENT_TO_DLQ = "cloudWatchLogsEventsSentToDlq";
    public static final String CLOUDWATCH_LOGS_REQUESTS_IN_FLIGHT = "cloudWatchLogsRequestsInFlight";
    public static final String CLOUDWATCH_LOGS_EXECUTOR_QUEUE_DEPTH = "cloudWatchLogsExecutorQueueDepth";
    public static final String CLOUDWATCH_LOGS_EXECUTOR_ACTIVE_THREADS = "cloudWatchLogsExecutorActiveThreads";
    public static final String CLOUDWATCH_LOGS_TEMPLATE_EXPRESSION_FAILED = "cloudWatchLogsTemplateExpressionFailed";
    public static final String CLOUDWATCH_LOGS_HEADER_VALUE_BLANK = "cloudWatchLogsHeaderValueBlank";
    public static final String CLOUDWATCH_LOGS_RESOURCE_NOT_FOUND_AFTER_CREATE = "cloudWatchLogsResourceNotFoundAfterCreate";
    public static final String CLOUDWATCH_LOGS_STAGED_AGE_AT_DELIVERY = "cloudWatchLogsStagedAgeAtDelivery";
    public static final String REASON_TAG = "reason";

    public static final String REASON_UNDELIVERABLE = "undeliverable";
    public static final String REASON_REJECTED = "rejected";
    public static final String REASON_RETRIES_EXHAUSTED = "retriesExhausted";
    public static final String REASON_PERMANENT_FAILURE = "permanentFailure";
    public static final String REASON_UNEXPECTED_ERROR = "unexpectedError";
    public static final String REASON_SHUTDOWN = "shutdown";
    public static final String REASON_DLQ_WRITE_FAILED = "dlqWriteFailed";
    static final List<String> RELEASED_WITHOUT_DELIVERY_REASONS = List.of(REASON_UNDELIVERABLE, REASON_REJECTED);
    static final List<String> SENT_TO_DLQ_REASONS =
            List.of(REASON_UNDELIVERABLE, REASON_REJECTED, REASON_RETRIES_EXHAUSTED, REASON_PERMANENT_FAILURE, REASON_UNEXPECTED_ERROR,
                    REASON_SHUTDOWN);
    static final List<String> NEGATIVELY_ACKED_REASONS =
            List.of(REASON_RETRIES_EXHAUSTED, REASON_PERMANENT_FAILURE, REASON_UNEXPECTED_ERROR, REASON_DLQ_WRITE_FAILED,
                    REASON_SHUTDOWN);
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
    private final Counter templateExpressionFailedCounter;
    private final Counter headerValueBlankCounter;
    private final Counter resourceNotFoundAfterCreateCounter;
    private final DistributionSummary stagedAgeAtDeliveryMetric;
    private final Map<String, Counter> releasedWithoutDeliveryCounters;
    private final Map<String, Counter> negativelyAckedCounters;
    private final Map<String, Counter> sentToDlqCounters;
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
        this.templateExpressionFailedCounter = pluginMetrics.counter(CLOUDWATCH_LOGS_TEMPLATE_EXPRESSION_FAILED);
        this.headerValueBlankCounter = pluginMetrics.counter(CLOUDWATCH_LOGS_HEADER_VALUE_BLANK);
        this.resourceNotFoundAfterCreateCounter = pluginMetrics.counter(CLOUDWATCH_LOGS_RESOURCE_NOT_FOUND_AFTER_CREATE);
        this.stagedAgeAtDeliveryMetric = pluginMetrics.summary(CLOUDWATCH_LOGS_STAGED_AGE_AT_DELIVERY);
        this.releasedWithoutDeliveryCounters = taggedCounters(CLOUDWATCH_LOGS_EVENTS_RELEASED_WITHOUT_DELIVERY, REASON_TAG,
                RELEASED_WITHOUT_DELIVERY_REASONS);
        this.negativelyAckedCounters = taggedCounters(CLOUDWATCH_LOGS_EVENTS_NEGATIVELY_ACKED, REASON_TAG, NEGATIVELY_ACKED_REASONS);
        this.sentToDlqCounters = taggedCounters(CLOUDWATCH_LOGS_EVENTS_SENT_TO_DLQ, REASON_TAG, SENT_TO_DLQ_REASONS);
    }

    /**
     * Pre-registers one counter per known tag value so the series exist at zero and the tag cardinality
     * is fixed by code. HashMap rather than Map.of because a mocked PluginMetrics returns null counters.
     */
    private Map<String, Counter> taggedCounters(final String name, final String tag, final List<String> values) {
        final Map<String, Counter> counters = new HashMap<>();
        for (final String value : values) {
            counters.put(value, pluginMetrics.counterWithTags(name, tag, value));
        }
        return Collections.unmodifiableMap(counters);
    }

    private static void incrementTagged(final Map<String, Counter> counters, final String tagValue, final int value) {
        final Counter counter = counters.get(tagValue);
        if (counter != null && value > 0) {
            counter.increment(value);
        }
    }

    /**
     * Counts events whose handle was released with a positive acknowledgement even though the event was
     * not delivered and no DLQ took it. Any non-zero value is acknowledged data loss.
     * Unknown reasons are ignored so a metrics call can never alter the release path.
     */
    public void increaseReleasedWithoutDeliveryCounter(final String reason, final int value) {
        incrementTagged(releasedWithoutDeliveryCounters, reason, value);
    }

    /**
     * Counts events whose handle was released with a negative acknowledgement, so the source may redeliver them.
     * Unknown reasons are ignored.
     */
    public void increaseEventsNegativelyAckedCounter(final String reason, final int value) {
        incrementTagged(negativelyAckedCounters, reason, value);
    }

    /**
     * Counts events released with a positive acknowledgement after the DLQ accepted them. Unknown reasons are ignored.
     */
    public void increaseEventsSentToDlqCounter(final String reason, final int value) {
        incrementTagged(sentToDlqCounters, reason, value);
    }

    /**
     * Registers the gauge for the number of PutLogEvents calls in progress. The gauge holds a weak reference to
     * {@code requestsInFlight}, so the caller must retain it.
     */
    public void registerRequestsInFlightGauge(final AtomicInteger requestsInFlight) {
        pluginMetrics.gauge(CLOUDWATCH_LOGS_REQUESTS_IN_FLIGHT, requestsInFlight, AtomicInteger::get);
    }

    /**
     * Registers queue-depth and active-thread gauges for the flusher pool. The gauges hold a weak
     * reference to {@code executor}, so the caller must retain it.
     */
    public void registerExecutorGauges(final ThreadPoolExecutor executor) {
        pluginMetrics.gauge(CLOUDWATCH_LOGS_EXECUTOR_QUEUE_DEPTH, executor, e -> e.getQueue().size());
        pluginMetrics.gauge(CLOUDWATCH_LOGS_EXECUTOR_ACTIVE_THREADS, executor, ThreadPoolExecutor::getActiveCount);
    }

    /**
     * Counts header, entity key-attribute, or entity attribute templates whose expression threw while resolving,
     * so the literal template was used. A template that resolves to an empty string is not counted here; see
     * {@link #increaseHeaderValueBlankCounter(int)}.
     */
    public void increaseTemplateExpressionFailedCounter(final int value) {
        templateExpressionFailedCounter.increment(value);
    }

    /**
     * Counts dynamic header values that resolved to a blank string for an event. The header is still sent as is.
     */
    public void increaseHeaderValueBlankCounter(final int value) {
        headerValueBlankCounter.increment(value);
    }

    /**
     * Counts ResourceNotFoundException on PutLogEvents after the sink already tried to create the log
     * group or stream. The pre-existing resource-not-found counter is not incremented on this path.
     */
    public void increaseResourceNotFoundAfterCreateCounter(final int value) {
        resourceNotFoundAfterCreateCounter.increment(value);
    }

    /**
     * Records, in milliseconds, the time from building the request to its delivery. InputLogEvent timestamps are set
     * when the request is built on the flusher pool thread, so this excludes buffer and pool-queue time and measures
     * how long retries and backoff held the request.
     */
    public void recordStagedAgeAtDelivery(final long ageMillis) {
        stagedAgeAtDeliveryMetric.record(ageMillis);
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

    public void increaseEntityDroppedCounter(final String reason) {
        pluginMetrics.counterWithTags(CLOUDWATCH_LOGS_ENTITY_DROPPED, REASON_TAG, reason).increment();
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
