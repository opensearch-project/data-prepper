/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.microsoft_office365;

import com.google.common.annotations.VisibleForTesting;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventType;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.CrawlerClient;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.PluginExecutorServiceProvider;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.PaginationCrawlerWorkerProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.ItemInfo;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.exception.Office365Exception;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.HttpClientErrorException;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.service.Office365Service;

import javax.inject.Named;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;

/**
 * Implementation of CrawlerClient for Office 365 audit logs.
 * This class manages the crawling process for Office 365 audit logs.
 */
@Slf4j
@Named
public class Office365CrawlerClient implements CrawlerClient<PaginationCrawlerWorkerProgressState> {
    private static final String BUFFER_WRITE_LATENCY = "bufferWriteLatency";
    private static final String BUFFER_WRITE_ATTEMPTS = "bufferWriteAttempts";
    private static final String BUFFER_WRITE_SUCCESS = "bufferWriteSuccess";
    private static final String BUFFER_WRITE_RETRY_SUCCESS = "bufferWriteRetrySuccess";
    private static final String BUFFER_WRITE_RETRY_ATTEMPTS = "bufferWriteRetryAttempts";
    private static final String BUFFER_WRITE_FAILURES = "bufferWriteFailures";
    private static final String WORKER_STATE_UPDATES = "workerStateUpdates";

    private static final String CONTENT_TYPE = "contentType";
    private static final int BUFFER_TIMEOUT_IN_SECONDS = 10;

    private final Office365Service service;
    private final Office365Iterator office365Iterator;
    private final ExecutorService executorService;
    private final Office365SourceConfig configuration;
    private final Timer bufferWriteLatencyTimer;
    private final Counter bufferWriteAttemptsCounter;
    private final Counter bufferWriteSuccessCounter;
    private final Counter bufferWriteRetrySuccessCounter;
    private final Counter bufferWriteRetryAttemptsCounter;
    private final Counter bufferWriteFailuresCounter;
    private final Counter workerStateUpdatesCounter;
    private ObjectMapper objectMapper;

    public Office365CrawlerClient(final Office365Service service,
                                  final Office365Iterator office365Iterator,
                                  final PluginExecutorServiceProvider executorServiceProvider,
                                  final Office365SourceConfig sourceConfig,
                                  final PluginMetrics pluginMetrics) {
        this.service = service;
        this.office365Iterator = office365Iterator;
        this.executorService = executorServiceProvider.get();
        this.configuration = sourceConfig;
        this.objectMapper = new ObjectMapper();

        // Initialize metrics
        this.bufferWriteLatencyTimer = pluginMetrics.timer(BUFFER_WRITE_LATENCY);
        this.bufferWriteAttemptsCounter = pluginMetrics.counter(BUFFER_WRITE_ATTEMPTS);
        this.bufferWriteSuccessCounter = pluginMetrics.counter(BUFFER_WRITE_SUCCESS);
        this.bufferWriteRetrySuccessCounter = pluginMetrics.counter(BUFFER_WRITE_RETRY_SUCCESS);
        this.bufferWriteRetryAttemptsCounter = pluginMetrics.counter(BUFFER_WRITE_RETRY_ATTEMPTS);
        this.bufferWriteFailuresCounter = pluginMetrics.counter(BUFFER_WRITE_FAILURES);
        this.workerStateUpdatesCounter = pluginMetrics.counter(WORKER_STATE_UPDATES);
    }

    @VisibleForTesting
    void injectObjectMapper(final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }


    @Override
    public Iterator<ItemInfo> listItems(final Instant lastPollTime) {
        log.info("Starting to list Office 365 audit logs from {}", lastPollTime);

        // TODO: Subscription management should be moved to a dedicated class in the future
        // Currently, we initialize subscriptions in the leader partition to ensure that we're always subscribed
        // to the required content type to ensure there hasn't been a subscription change
        service.initializeSubscriptions();
        office365Iterator.initialize(lastPollTime);
        return office365Iterator;
    }

    @Override
    public void executePartition(final PaginationCrawlerWorkerProgressState state,
                                 final Buffer<Record<Event>> buffer,
                                 final AcknowledgementSet acknowledgementSet) {
        // Process a batch of audit log IDs and convert them to records
        // If any record fails to process, the entire batch will be retried
        log.info("Starting to execute partition with {} log(s)", state.getItemIds().size());
        List<String> itemIds = state.getItemIds();

        // Process each audit log ID in the batch
        List<Record<Event>> records = itemIds.stream()
                .map(id -> {
                    try {
                        return processAuditLog(id);
                    } catch (Office365Exception e) {
                        log.error(NOISY, "{} error processing audit log: {}",
                                e.isRetryable() ? "Retryable" : "Non-retryable", id, e);
                        if (e.isRetryable()) {
                            throw new RuntimeException("Retryable error processing audit log: " + id, e);
                        } else {
                            // TODO: When pipeline DLQ is ready, add this record to DLQ instead of dropping the record
                            log.error(NOISY, "Non-retryable error - record will be dropped. Error processing audit log: {}", id, e);
                            return null;
                        }
                    } catch (Exception e) {
                        // Unexpected errors are treated as retryable to be safe
                        log.error(NOISY, "Unexpected error processing audit log: {}", id, e);
                        throw new RuntimeException("Unexpected error processing audit log: " + id, e);
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        bufferWriteLatencyTimer.record(() -> {
            try {
                writeRecordsWithRetry(records, buffer, acknowledgementSet, state);
            } catch (Exception e) {
                bufferWriteFailuresCounter.increment();
                throw e;
            }
        });
    }

    private Record<Event> processAuditLog(String id) {
        try {
            String auditLog = service.getAuditLog(id);

            // Handle HTTP errors in service layer
            if (auditLog == null) {
                throw new Office365Exception("Received null audit log for ID: " + id, false);
            }

            try {
                JsonNode jsonNode = objectMapper.readTree(auditLog);
                Map<String, Object> data;

                // Office 365 API sometimes returns an array with a single item
                // and sometimes returns a single object directly
                if (jsonNode.isArray() && !jsonNode.isEmpty()) {
                    data = objectMapper.convertValue(jsonNode.get(0), new TypeReference<Map<String, Object>>() {});
                } else {
                    data = objectMapper.readValue(auditLog, new TypeReference<Map<String, Object>>() {});
                }

                // "Workload" is an Office 365 specific field that indicates the source of the audit log
                String contentType = (String) data.get("Workload");
                if (contentType == null) {
                    throw new Office365Exception("Missing Workload field in audit log: " + id, false);
                }

                Event event = JacksonEvent.builder()
                        .withEventType(EventType.LOG.toString())
                        .withData(data)
                        .build();
                event.getMetadata().setAttribute(CONTENT_TYPE, contentType);
                return new Record<>(event);
            } catch (JsonProcessingException e) {
                // JSON parsing errors are non-retryable as they indicate malformed data
                throw new Office365Exception("Failed to parse audit log: " + id, e, false);
            }
        } catch (HttpClientErrorException e) {
            switch (e.getStatusCode()) {
                case UNAUTHORIZED:
                case FORBIDDEN:
                    // Auth errors might be temporary due to token expiration
                    throw new Office365Exception("Authentication failed while fetching audit log: " + id, e, true);
                case NOT_FOUND:
                    // Log doesn't exist - non-retryable
                    throw new Office365Exception("Audit log not found: " + id, e, false);
                case TOO_MANY_REQUESTS:
                    // Rate limiting - retryable
                    throw new Office365Exception("Rate limited while fetching audit log: " + id, e, true);
                default:
                    // Other client errors are non-retryable
                    throw new Office365Exception("Client error while fetching audit log: " + id, e, false);
            }
        } catch (ResourceAccessException e) {
            // Network/connection issues are retryable
            throw new Office365Exception("Network error while fetching audit log: " + id, e, true);
        }
    }

    private void writeRecordsWithRetry(final List<Record<Event>> records,
                                       final Buffer<Record<Event>> buffer,
                                       final AcknowledgementSet acknowledgementSet,
                                       final PaginationCrawlerWorkerProgressState state) {
        bufferWriteAttemptsCounter.increment();
        int retryCount = 0;
        int currentBackoff = 1000; // Start with 1 second
        final int maxBackoff = 30000; // Max 30 seconds
        final int maxRetries = 1000; // Keep retrying to write to the buffer

        while (true) {
            try {
                if (configuration.isAcknowledgments()) {
                    records.forEach(record -> acknowledgementSet.add(record.getData()));
                    buffer.writeAll(records, (int) Duration.ofSeconds(BUFFER_TIMEOUT_IN_SECONDS).toMillis());
                    acknowledgementSet.complete();
                } else {
                    buffer.writeAll(records, (int) Duration.ofSeconds(BUFFER_TIMEOUT_IN_SECONDS).toMillis());
                }

                if (retryCount > 0) {
                    bufferWriteRetrySuccessCounter.increment();
                } else {
                    bufferWriteSuccessCounter.increment();
                }
                return;

            } catch (TimeoutException e) {
                retryCount++;
                if (retryCount >= maxRetries) {
                    bufferWriteFailuresCounter.increment();
                    throw new RuntimeException("Failed to write to buffer after " + maxRetries + " attempts", e);
                }

                bufferWriteRetryAttemptsCounter.increment();
                currentBackoff = Math.min((int)(currentBackoff * 2.0), maxBackoff);
                log.info("Buffer full, backing off for {} ms before retry", currentBackoff);

                try {
                    Thread.sleep(currentBackoff);

                    // Update worker state to prevent timeout
                    Instant currentTime = Instant.now();
                    state.setExportStartTime(currentTime);
                    workerStateUpdatesCounter.increment();

                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Buffer write retry interrupted", ie);
                }
            } catch (Exception e) {
                bufferWriteFailuresCounter.increment();
                throw new RuntimeException("Error writing to buffer", e);
            }
        }
    }
}
