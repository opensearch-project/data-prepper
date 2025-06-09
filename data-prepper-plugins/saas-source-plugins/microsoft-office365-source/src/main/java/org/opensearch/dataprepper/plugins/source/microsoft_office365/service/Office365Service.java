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
import org.opensearch.dataprepper.plugins.source.microsoft_office365.configuration.Office365ItemInfo;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.ItemInfo;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.models.AuditLogsResponse;

import javax.inject.Named;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;
import static org.opensearch.dataprepper.plugins.source.microsoft_office365.utils.Constants.CONTENT_TYPES;

/**
 * Service class for managing Office 365 audit log retrieval.
 */
@Slf4j
@Named
public class Office365Service {
    private final Office365SourceConfig office365SourceConfig;
    private final Office365RestClient office365RestClient;
    private final Counter searchResultsFoundCounter;
    private final Counter sevenDayLimitHitCounter;
    private final Counter daysAdjustedCounter;
    private final Counter windowRetryCounter;
    private static final String CONTENT_ID_KEY = "contentId";
    private static final String CONTENT_CREATED_KEY = "contentCreated";
    private static final String CONTENT_URI_KEY = "contentUri";
    private static final String TYPE_KEY = "type";
    private static final Duration TIME_WINDOW = Duration.ofHours(1);
    private static final Duration RETRY_DELAY = Duration.ofSeconds(1);

    public Office365Service(final Office365SourceConfig office365SourceConfig,
                            final Office365RestClient office365RestClient,
                            final PluginMetrics pluginMetrics) {
        this.office365SourceConfig = office365SourceConfig;
        this.office365RestClient = office365RestClient;
        this.searchResultsFoundCounter = pluginMetrics.counter("searchResultsFound");
        this.sevenDayLimitHitCounter = pluginMetrics.counter("sevenDayLimitHit");
        this.daysAdjustedCounter = pluginMetrics.counter("daysAdjusted");
        this.windowRetryCounter = pluginMetrics.counter("windowRetry");
    }

    public void initializeSubscriptions() {
        office365RestClient.startSubscriptions();
    }

    public void getOffice365Entities(final Instant timestamp,
                                     final Queue<ItemInfo> itemInfoQueue) {
        log.trace("Started to fetch entities");
        searchForNewLogs(timestamp, itemInfoQueue);
        log.trace("Creating item information and adding in queue");
    }

    public String getAuditLog(final String contentId) {
        return office365RestClient.getAuditLog(contentId);
    }

    private void searchForNewLogs(final Instant timestamp,
                                  final Queue<ItemInfo> itemInfoQueue) {
        Instant endTime = Instant.now();
        log.info("Searching for logs between {} and {}", timestamp, endTime);
        Instant startTime = timestamp;

        Instant sevenDaysAgo = endTime.minus(Duration.ofDays(7));
        if (startTime.isBefore(sevenDaysAgo)) {
            long daysAdjusted = Duration.between(startTime, sevenDaysAgo).toDays();
            sevenDayLimitHitCounter.increment(); // Track that we hit the limit
            daysAdjustedCounter.increment(daysAdjusted); // Track by how many days
            log.warn("Adjusting start time from {} to {} ({} days beyond 7-day limit)",
                    startTime, sevenDaysAgo, daysAdjusted);
            startTime = sevenDaysAgo;
        }

        while (startTime.isBefore(endTime)) {
            Instant windowEnd = startTime.plus(TIME_WINDOW);
            if (windowEnd.isAfter(endTime)) {
                windowEnd = endTime;
            }
            log.debug("Processing time window: {} to {}", startTime, windowEnd);
            boolean windowSuccessful = true;
            for (String contentType : CONTENT_TYPES) {
                String nextPageUri = null;
                try {
                    do {
                        AuditLogsResponse response =
                                office365RestClient.searchAuditLogs(contentType, startTime, windowEnd, nextPageUri);

                        if (response.getItems() == null || response.getItems().isEmpty()) {
                            break;
                        }

                        addItemsToQueue(response.getItems(), contentType, itemInfoQueue);
                        nextPageUri = response.getNextPageUri();

                    } while (nextPageUri != null);
                } catch (Exception e) {
                    log.error(NOISY, "Failed to fetch logs for time window {} to {} for content type {}. Will retry this window.",
                            startTime, windowEnd, contentType, e);
                    windowSuccessful = false;
                    windowRetryCounter.increment();
                    break; // Exit the content type loop on failure
                }
            }

            // Only move the pointer if all content types were processed successfully
            if (windowSuccessful) {
                log.trace("Successfully completed time window: {} to {}, moving to next window", startTime, windowEnd);
                startTime = windowEnd;
            } else {
                log.error("Failed to complete time window: {} to {}, retrying after delay", startTime, windowEnd);
                // Add a small delay before retrying the same window
                try {
                    Thread.sleep(RETRY_DELAY.toMillis());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting to retry time window", ie);
                }
            }
        }
    }

    private void addItemsToQueue(final List<Map<String, Object>> items,
                                 final String contentType,
                                 final Queue<ItemInfo> itemInfoQueue) {
        items.forEach(item -> {
            ItemInfo itemInfo = Office365ItemInfo.builder()
                    .itemId((String) item.get(CONTENT_ID_KEY))
                    .eventTime(Instant.parse((String) item.get(CONTENT_CREATED_KEY)))
                    .partitionKey(contentType + UUID.randomUUID())
                    .metadata(item)
                    .keyAttributes(Map.of(TYPE_KEY, contentType, CONTENT_URI_KEY, item.get(CONTENT_URI_KEY)))
                    .lastModifiedAt(Instant.now()) // Used to track the time that it was imported
                    .build();
            itemInfoQueue.add(itemInfo);
        });
        searchResultsFoundCounter.increment(items.size());
    }
}
