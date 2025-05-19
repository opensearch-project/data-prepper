/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.office365;

import io.micrometer.core.instrument.Counter;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.source.office365.configuration.Office365ItemInfo;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.ItemInfo;
import org.opensearch.dataprepper.plugins.source.office365.models.AuditLogsResponse;

import javax.inject.Named;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import static org.opensearch.dataprepper.plugins.source.office365.utils.Constants.CONTENT_TYPES;

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
    private static final String CONTENT_ID_KEY = "contentId";
    private static final String CONTENT_CREATED_KEY = "contentCreated";
    private static final String CONTENT_URI_KEY = "contentUri";
    private static final String TYPE_KEY = "type";
    private static final Duration TIME_WINDOW = Duration.ofHours(1);

    public Office365Service(final Office365SourceConfig office365SourceConfig,
                            final Office365RestClient office365RestClient,
                            final PluginMetrics pluginMetrics) {
        this.office365SourceConfig = office365SourceConfig;
        this.office365RestClient = office365RestClient;
        this.searchResultsFoundCounter = pluginMetrics.counter("searchResultsFound");
        this.sevenDayLimitHitCounter = pluginMetrics.counter("sevenDayLimitHit");
        this.daysAdjustedCounter = pluginMetrics.counter("daysAdjusted");
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
        log.trace("Looking for new logs with a Search API call");
        Instant endTime = Instant.now();
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

            for (String contentType : CONTENT_TYPES) {
                String nextPageUri = null;

                do {
                    AuditLogsResponse response =
                            office365RestClient.searchAuditLogs(contentType, startTime, windowEnd, nextPageUri);

                    if (response.getItems() == null || response.getItems().isEmpty()) {
                        break;
                    }

                    addItemsToQueue(response.getItems(), contentType, itemInfoQueue);
                    nextPageUri = response.getNextPageUri();

                } while (nextPageUri != null);
            }

            startTime = windowEnd;
        }
    }

    private void addItemsToQueue(final List<Map<String, Object>> items,
                                 final String contentType,
                                 final Queue<ItemInfo> itemInfoQueue) {
        items.forEach(item -> {
            ItemInfo itemInfo = Office365ItemInfo.builder()
                    .itemId((String) item.get(CONTENT_ID_KEY))
                    .eventTime(Instant.parse((String) item.get(CONTENT_CREATED_KEY)))
                    .partitionKey(contentType)
                    .metadata(item)
                    .keyAttributes(Map.of(TYPE_KEY, contentType, CONTENT_URI_KEY, item.get(CONTENT_URI_KEY)))
                    .lastModifiedAt(Instant.parse((String) item.get(CONTENT_CREATED_KEY)))
                    .build();
            itemInfoQueue.add(itemInfo);
        });
        searchResultsFoundCounter.increment(items.size());
    }
}
