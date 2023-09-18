/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.s3;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.metrics.PluginMetrics;

public class S3ObjectPluginMetrics {
    static final String S3_OBJECTS_SIZE_PROCESSED = "s3ObjectProcessedBytes";
    static final String S3_OBJECTS_FAILED_METRIC_NAME = "s3ObjectsFailed";
    static final String S3_OBJECTS_SUCCEEDED_METRIC_NAME = "s3ObjectsSucceeded";
    static final String S3_OBJECTS_EVENTS = "s3ObjectsEvents";
    static final String S3_OBJECTS_FAILED_NOT_FOUND_METRIC_NAME = "s3ObjectsNotFound";
    static final String S3_OBJECTS_FAILED_NOT_FOUND_ACCESS_DENIED = "s3ObjectsAccessDenied";
    static final String S3_OBJECTS_TIME_ELAPSED_METRIC_NAME = "s3ObjectReadTimeElapsed";
    static final String S3_OBJECTS_SIZE = "s3ObjectSizeBytes";
    static final String S3_OBJECTS_NO_RECORDS_FOUND = "s3ObjectNoRecordsFound";
    private final Counter s3ObjectsFailedCounter;
    private final Counter s3ObjectsFailedNotFoundCounter;
    private final Counter s3ObjectsFailedAccessDeniedCounter;
    private final Counter s3ObjectsSucceededCounter;
    private final Timer s3ObjectReadTimer;
    private final DistributionSummary s3ObjectSizeSummary;
    private final DistributionSummary s3ObjectSizeProcessedSummary;
    private final DistributionSummary s3ObjectEventsSummary;
    private final Counter s3ObjectNoRecordsFound;

    public S3ObjectPluginMetrics(final PluginMetrics pluginMetrics){
        s3ObjectsFailedCounter = pluginMetrics.counter(S3_OBJECTS_FAILED_METRIC_NAME);
        s3ObjectsFailedNotFoundCounter = pluginMetrics.counter(S3_OBJECTS_FAILED_NOT_FOUND_METRIC_NAME);
        s3ObjectsFailedAccessDeniedCounter = pluginMetrics.counter(S3_OBJECTS_FAILED_NOT_FOUND_ACCESS_DENIED);
        s3ObjectsSucceededCounter = pluginMetrics.counter(S3_OBJECTS_SUCCEEDED_METRIC_NAME);
        s3ObjectReadTimer = pluginMetrics.timer(S3_OBJECTS_TIME_ELAPSED_METRIC_NAME);
        s3ObjectSizeSummary = pluginMetrics.summary(S3_OBJECTS_SIZE);
        s3ObjectSizeProcessedSummary = pluginMetrics.summary(S3_OBJECTS_SIZE_PROCESSED);
        s3ObjectEventsSummary = pluginMetrics.summary(S3_OBJECTS_EVENTS);
        s3ObjectNoRecordsFound = pluginMetrics.counter(S3_OBJECTS_NO_RECORDS_FOUND);
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
}