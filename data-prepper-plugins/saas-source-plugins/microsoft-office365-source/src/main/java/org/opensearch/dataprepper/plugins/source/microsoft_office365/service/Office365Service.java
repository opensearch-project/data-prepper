/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.microsoft_office365.service;

import io.micrometer.core.instrument.Counter;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.Office365RestClient;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.Office365SourceConfig;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.exception.Office365Exception;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.models.AuditLogsResponse;

import javax.inject.Named;
import java.time.Duration;
import java.time.Instant;

@Slf4j
@Named
public class Office365Service {
    private final Office365SourceConfig office365SourceConfig;
    private final Office365RestClient office365RestClient;
    private final Counter searchResultsFoundCounter;
    private final Counter windowRetryCounter;
    private static final Duration SEVEN_DAYS = Duration.ofDays(7);

    public Office365Service(final Office365SourceConfig office365SourceConfig,
                            final Office365RestClient office365RestClient,
                            final PluginMetrics pluginMetrics) {
        this.office365SourceConfig = office365SourceConfig;
        this.office365RestClient = office365RestClient;
        this.searchResultsFoundCounter = pluginMetrics.counter("searchResultsFound");
        this.windowRetryCounter = pluginMetrics.counter("windowRetry");
    }

    public void initializeSubscriptions() {
        office365RestClient.startSubscriptions();
    }

    public String getAuditLog(String contentUri) {
        return office365RestClient.getAuditLog(contentUri);
    }

    public AuditLogsResponse searchAuditLogs(final String logType,
                                             Instant startTime,
                                             final Instant endTime,
                                             final String nextPageUri) {
        if (startTime == null || endTime == null) {
            throw new IllegalArgumentException("startTime and endTime must not be null");
        }
        if (logType == null) {
            throw new IllegalArgumentException("logType must not be null");
        }
        try {
            // If pagination URI exists, use it directly
            if (nextPageUri != null) {
                return office365RestClient.searchAuditLogs(logType, startTime, endTime, nextPageUri);
            }

            // Check and adjust start time for 7-day limit
            Instant adjustedStartTime = startTime;
            Instant sevenDaysAgo = Instant.now().minus(SEVEN_DAYS);
            if (startTime.isBefore(sevenDaysAgo)) {
                adjustedStartTime = sevenDaysAgo;
            }

            AuditLogsResponse response =
                    office365RestClient.searchAuditLogs(logType, adjustedStartTime, endTime, null);
            if (response.getItems() != null) {
                searchResultsFoundCounter.increment(response.getItems().size());
            }
            return response;
        } catch (Exception e) {
            windowRetryCounter.increment();
            throw new Office365Exception(
                    String.format("Failed to fetch logs for time window %s to %s for log type %s.",
                            startTime, endTime, logType), e, true);
        }
    }
}
