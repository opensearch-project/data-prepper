/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.s3_enricher.processor.s3source;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.metrics.PluginMetrics;

public class S3EnricherObjectPluginMetrics {
    static final String S3_ENRICHER_OBJECTS_SIZE_PROCESSED = "s3EnricherObjectProcessedBytes";
    static final String S3_ENRICHER_OBJECTS_FAILED_METRIC_NAME = "s3EnricherObjectsFailed";
    static final String S3_ENRICHER_OBJECTS_DELETE_FAILED_METRIC_NAME = "s3EnricherObjectsDeleteFailed";
    static final String S3_ENRICHER_OBJECTS_SUCCEEDED_METRIC_NAME = "s3EnricherObjectsSucceeded";
    static final String S3_ENRICHER_OBJECTS_EVENTS = "s3EnricherObjectsEvents";
    static final String S3_ENRICHER_OBJECTS_FAILED_NOT_FOUND_METRIC_NAME = "s3EnricherObjectsNotFound";
    static final String S3_ENRICHER_OBJECTS_FAILED_NOT_FOUND_ACCESS_DENIED = "s3EnricherObjectsAccessDenied";
    static final String S3_ENRICHER_OBJECTS_TIME_ELAPSED_METRIC_NAME = "s3EnricherObjectReadTimeElapsed";
    static final String S3_ENRICHER_OBJECTS_SIZE = "s3EnricherObjectSizeBytes";
    static final String S3_ENRICHER_OBJECTS_NO_RECORDS_FOUND = "s3EnricherObjectNoRecordsFound";
    static final String S3_ENRICHER_OBJECTS_THROTTLED_METRIC_NAME = "s3EnricherObjectsThrottled";
    private final Counter s3ObjectsFailedCounter;
    private final Counter s3ObjectsThrottledCounter;
    private final Counter s3ObjectsFailedNotFoundCounter;
    private final Counter s3ObjectsFailedAccessDeniedCounter;
    private final Counter s3ObjectsSucceededCounter;
    private final Timer s3ObjectReadTimer;
    private final DistributionSummary s3ObjectSizeSummary;
    private final DistributionSummary s3ObjectSizeProcessedSummary;
    private final DistributionSummary s3ObjectEventsSummary;
    private final Counter s3ObjectNoRecordsFound;

    private final Counter s3ObjectsDeleteFailed;

    public S3EnricherObjectPluginMetrics(final PluginMetrics pluginMetrics){
        s3ObjectsFailedCounter = pluginMetrics.counter(S3_ENRICHER_OBJECTS_FAILED_METRIC_NAME);
        s3ObjectsThrottledCounter = pluginMetrics.counter(S3_ENRICHER_OBJECTS_THROTTLED_METRIC_NAME);
        s3ObjectsFailedNotFoundCounter = pluginMetrics.counter(S3_ENRICHER_OBJECTS_FAILED_NOT_FOUND_METRIC_NAME);
        s3ObjectsFailedAccessDeniedCounter = pluginMetrics.counter(S3_ENRICHER_OBJECTS_FAILED_NOT_FOUND_ACCESS_DENIED);
        s3ObjectsSucceededCounter = pluginMetrics.counter(S3_ENRICHER_OBJECTS_SUCCEEDED_METRIC_NAME);
        s3ObjectReadTimer = pluginMetrics.timer(S3_ENRICHER_OBJECTS_TIME_ELAPSED_METRIC_NAME);
        s3ObjectSizeSummary = pluginMetrics.summary(S3_ENRICHER_OBJECTS_SIZE);
        s3ObjectSizeProcessedSummary = pluginMetrics.summary(S3_ENRICHER_OBJECTS_SIZE_PROCESSED);
        s3ObjectEventsSummary = pluginMetrics.summary(S3_ENRICHER_OBJECTS_EVENTS);
        s3ObjectNoRecordsFound = pluginMetrics.counter(S3_ENRICHER_OBJECTS_NO_RECORDS_FOUND);
        s3ObjectsDeleteFailed = pluginMetrics.counter(S3_ENRICHER_OBJECTS_DELETE_FAILED_METRIC_NAME);
    }

    public Counter getS3ObjectsFailedCounter() {
        return s3ObjectsFailedCounter;
    }

    public Counter getS3ObjectsFailedNotFoundCounter() {
        return s3ObjectsFailedNotFoundCounter;
    }

    public Counter getS3ObjectsFailedAccessDeniedCounter() {
        return s3ObjectsFailedAccessDeniedCounter;
    }

    public Counter getS3ObjectsSucceededCounter() {
        return s3ObjectsSucceededCounter;
    }

    public Timer getS3ObjectReadTimer() {
        return s3ObjectReadTimer;
    }

    public DistributionSummary getS3ObjectSizeSummary() {
        return s3ObjectSizeSummary;
    }

    public DistributionSummary getS3ObjectSizeProcessedSummary() {
        return s3ObjectSizeProcessedSummary;
    }

    public DistributionSummary getS3ObjectEventsSummary() {
        return s3ObjectEventsSummary;
    }

    public Counter getS3ObjectNoRecordsFound() {
        return s3ObjectNoRecordsFound;
    }

    public Counter getS3ObjectsThrottledCounter() {
        return s3ObjectsThrottledCounter;
    }

    public Counter getS3ObjectsDeleteFailed() { return s3ObjectsDeleteFailed; }
}