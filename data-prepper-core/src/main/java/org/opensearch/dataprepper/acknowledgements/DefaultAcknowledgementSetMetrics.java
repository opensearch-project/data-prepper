/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.acknowledgements;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import io.micrometer.core.instrument.Counter;

public class DefaultAcknowledgementSetMetrics {
    static final String CREATED_METRIC_NAME = "numberOfAcknowledgementSetsCreated";
    static final String COMPLETED_METRIC_NAME = "numberOfAcknowledgementSetsCompleted";
    static final String EXPIRED_METRIC_NAME = "numberOfAcknowledgementSetsExpired";
    static final String INVALID_ACQUIRES_METRIC_NAME = "numberOfInvalidAcknowledgementAcquires";
    static final String INVALID_RELEASES_METRIC_NAME = "numberOfInvalidAcknowledgementReleases";
    private final Counter numberOfAcknowledgementSetsCreated;
    private final Counter numberOfAcknowledgementSetsCompleted;
    private final Counter numberOfAcknowledgementSetsExpired;
    private final Counter numberOfInvalidAcknowledgementAcquires;
    private final Counter numberOfInvalidAcknowledgementReleases;

    public DefaultAcknowledgementSetMetrics(PluginMetrics pluginMetrics) {
        numberOfAcknowledgementSetsCreated = pluginMetrics.counter(CREATED_METRIC_NAME);
        numberOfAcknowledgementSetsCompleted = pluginMetrics.counter(COMPLETED_METRIC_NAME);
        numberOfAcknowledgementSetsExpired = pluginMetrics.counter(EXPIRED_METRIC_NAME);
        numberOfInvalidAcknowledgementAcquires = pluginMetrics.counter(INVALID_ACQUIRES_METRIC_NAME);
        numberOfInvalidAcknowledgementReleases = pluginMetrics.counter(INVALID_RELEASES_METRIC_NAME);
    }
    
    public void increment(String metricName) throws IllegalArgumentException {
        switch (metricName) {
            case CREATED_METRIC_NAME:
                numberOfAcknowledgementSetsCreated.increment();
                break;
            case COMPLETED_METRIC_NAME:
                numberOfAcknowledgementSetsCompleted.increment();
                break;
            case EXPIRED_METRIC_NAME:
                numberOfAcknowledgementSetsExpired.increment();
                break;
            case INVALID_ACQUIRES_METRIC_NAME:
                numberOfInvalidAcknowledgementAcquires.increment();
                break;
            case INVALID_RELEASES_METRIC_NAME:
                numberOfInvalidAcknowledgementReleases.increment();
                break;
            default:
                throw new IllegalArgumentException("Invalid metric name");
        }
    }

}

