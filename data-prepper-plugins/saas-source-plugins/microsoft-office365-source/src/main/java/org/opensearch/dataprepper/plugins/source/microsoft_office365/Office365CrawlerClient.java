/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.microsoft_office365;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
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
import org.opensearch.dataprepper.plugins.source.source_crawler.exception.SaaSCrawlerException;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.CrawlerClient;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.DimensionalTimeSliceWorkerProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.ItemInfo;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.service.Office365Service;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.models.AuditLogsResponse;

import javax.inject.Named;
import java.time.Duration;
import java.time.Instant;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Iterator;
import java.util.concurrent.TimeoutException;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;
import static org.opensearch.dataprepper.plugins.source.source_crawler.utils.MetricsHelper.REQUEST_ERRORS;

/**
 * Implementation of CrawlerClient for Office 365 audit logs.
 * This class manages the crawling process for Office 365 audit logs.
 */
@Slf4j
@Named
public class Office365CrawlerClient implements CrawlerClient<DimensionalTimeSliceWorkerProgressState> {

    public static final String NON_RETRYABLE_ERRORS = "nonRetryableErrors";
    public static final String RETRYABLE_ERRORS = "retryableErrors";

    private static final String BUFFER_WRITE_LATENCY = "bufferWriteLatency";
    private static final String BUFFER_WRITE_ATTEMPTS = "bufferWriteAttempts";
    private static final String BUFFER_WRITE_SUCCESS = "bufferWriteSuccess";
    private static final String BUFFER_WRITE_RETRY_SUCCESS = "bufferWriteRetrySuccess";
    private static final String BUFFER_WRITE_RETRY_ATTEMPTS = "bufferWriteRetryAttempts";
    private static final String BUFFER_WRITE_FAILURES = "bufferWriteFailures";
    private static final int BUFFER_TIMEOUT_IN_SECONDS = 10;
    private static final String CONTENT_ID = "contentId";
    private static final String CONTENT_URI = "contentUri";

    private final Office365Service service;
    private final Office365SourceConfig configuration;
    private final Timer bufferWriteLatencyTimer;
    private final Counter bufferWriteAttemptsCounter;
    private final Counter bufferWriteSuccessCounter;
    private final Counter bufferWriteRetrySuccessCounter;
    private final Counter bufferWriteRetryAttemptsCounter;
    private final Counter bufferWriteFailuresCounter;
    private final Counter requestErrorsCounter;
    private final Counter nonRetryableErrorsCounter;
    private final Counter retryableErrorsCounter;
    private ObjectMapper objectMapper;

    public Office365CrawlerClient(final Office365Service service,
                                  final Office365SourceConfig sourceConfig,
                                  final PluginMetrics pluginMetrics) {
        this.service = service;
        this.configuration = sourceConfig;
        this.objectMapper = new ObjectMapper();

        // Initialize metrics
        this.bufferWriteLatencyTimer = pluginMetrics.timer(BUFFER_WRITE_LATENCY);
        this.bufferWriteAttemptsCounter = pluginMetrics.counter(BUFFER_WRITE_ATTEMPTS);
        this.bufferWriteSuccessCounter = pluginMetrics.counter(BUFFER_WRITE_SUCCESS);
        this.bufferWriteRetrySuccessCounter = pluginMetrics.counter(BUFFER_WRITE_RETRY_SUCCESS);
        this.bufferWriteRetryAttemptsCounter = pluginMetrics.counter(BUFFER_WRITE_RETRY_ATTEMPTS);
        this.bufferWriteFailuresCounter = pluginMetrics.counter(BUFFER_WRITE_FAILURES);
        this.requestErrorsCounter = pluginMetrics.counter(REQUEST_ERRORS);
        this.nonRetryableErrorsCounter = pluginMetrics.counter(NON_RETRYABLE_ERRORS);
        this.retryableErrorsCounter = pluginMetrics.counter(RETRYABLE_ERRORS);
    }

    @VisibleForTesting
    void injectObjectMapper(final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public Iterator<ItemInfo> listItems(final Instant lastPollTime) {
        return null;
    }

    @Override
    public void executePartition(final DimensionalTimeSliceWorkerProgressState state,
                                 final Buffer<Record<Event>> buffer,
                                 final AcknowledgementSet acknowledgementSet) {
        final Instant startTime = state.getStartTime();
        final Instant endTime = state.getEndTime();
        final String logType = state.getDimensionType();

        try {
            String nextPageUri = null;

            do {
                List<Record<Event>> records = new ArrayList<>();
                AuditLogsResponse response =
                        service.searchAuditLogs(logType, startTime, endTime, nextPageUri);

                if (response.getItems() != null && !response.getItems().isEmpty()) {
                    for (Map<String, Object> metadata : response.getItems()) {
                        String logId = (String) metadata.get(CONTENT_ID);
                        try {
                            Record<Event> record = processAuditLog(metadata);
                            if (record != null) {
                                records.add(record);
                            }
                        } catch (SaaSCrawlerException e) {
                            boolean isRetryable = e.isRetryable();
                            log.error(NOISY, "{} error processing audit log: {}",
                                    isRetryable ? "Retryable" : "Non-retryable", logId, e);
                            throw new SaaSCrawlerException("Error processing audit log: " + logId, e, isRetryable);
                        }
                    }
                }

                // Write Records to the buffer after processing a page of data
                bufferWriteLatencyTimer.record(() -> {
                    try {
                        writeRecordsWithRetry(records, buffer, acknowledgementSet);
                    } catch (Exception e) {
                        bufferWriteFailuresCounter.increment();
                        throw e;
                    }
                });

                nextPageUri = response.getNextPageUri();
            } while (nextPageUri != null);

            if (configuration.isAcknowledgments()) {
                acknowledgementSet.complete();
            }
        } catch (Exception e) {
            log.error(NOISY, "Failed to process partition for log type {} from {} to {}",
                    logType, startTime, endTime, e);
            requestErrorsCounter.increment();
            if (e instanceof SaaSCrawlerException) {
                SaaSCrawlerException saasException = (SaaSCrawlerException) e;
                if (saasException.isRetryable()) {
                    retryableErrorsCounter.increment();
                } else {
                    nonRetryableErrorsCounter.increment();
                }
                throw e;
            }
            // any other exceptions = non-retryable
            nonRetryableErrorsCounter.increment();
            throw new SaaSCrawlerException("Failed to process partition", e, false);
        }
    }

    private Record<Event> processAuditLog(Map<String, Object> metadata) throws SaaSCrawlerException {
        String contentUri = (String) metadata.get(CONTENT_URI);
        if (contentUri == null) {
            throw new SaaSCrawlerException("Missing contentUri in metadata", false);
        }

        String logContent = service.getAuditLog(contentUri);
        if (logContent == null) {
            throw new SaaSCrawlerException("Received null log content for URI: " + contentUri, false);
        }
        String logId = (String) metadata.get(CONTENT_ID);

        try {
            JsonNode jsonNode = objectMapper.readTree(logContent);
            Map<String, Object> data;

            // Office 365 API sometimes returns an array with a single item
            // and sometimes returns a single object directly
            if (jsonNode.isArray() && !jsonNode.isEmpty()) {
                data = objectMapper.convertValue(jsonNode.get(0), new TypeReference<Map<String, Object>>() {});
            } else {
                data = objectMapper.readValue(logContent, new TypeReference<Map<String, Object>>() {});
            }

            String contentType = (String) data.get("Workload");
            if (contentType == null) {
                throw new SaaSCrawlerException("Missing Workload field in audit log: " + logId, false);
            }

            Event event = JacksonEvent.builder()
                    .withEventType(EventType.LOG.toString())
                    .withData(data)
                    .build();
            event.getMetadata().setAttribute("contentType", contentType);
            return new Record<>(event);
        } catch (JsonProcessingException e) {
            // JSON parsing errors are non-retryable as they indicate malformed data
            throw new SaaSCrawlerException("Failed to parse audit log: " + logId, e, false);
        }
    }

    private void writeRecordsWithRetry(final List<Record<Event>> records,
                                       final Buffer<Record<Event>> buffer,
                                       final AcknowledgementSet acknowledgementSet) {
        bufferWriteAttemptsCounter.increment();
        int retryCount = 0;
        int currentBackoff = 1000; // Start with 1 second
        final int maxBackoff = 30000; // Max 30 seconds
        final int maxRetries = 5;

        while (true) {
            try {
                if (configuration.isAcknowledgments()) {
                    records.forEach(record -> acknowledgementSet.add(record.getData()));
                    buffer.writeAll(records, (int) Duration.ofSeconds(BUFFER_TIMEOUT_IN_SECONDS).toMillis());
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
                    // allows all writeToBuffer exceptions to be retryable to keep current behaviour of immediate retry by WorkerScheduler
                    throw new SaaSCrawlerException("Failed to write to buffer after " + maxRetries + " attempts", e, true);
                }

                bufferWriteRetryAttemptsCounter.increment();
                currentBackoff = Math.min((int)(currentBackoff * 2.0), maxBackoff);
                log.info("Buffer full, backing off for {} ms before retry", currentBackoff);

                try {
                    Thread.sleep(currentBackoff);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new SaaSCrawlerException("Buffer write retry interrupted", ie, true);
                }
            } catch (Exception e) {
                bufferWriteFailuresCounter.increment();
                throw new SaaSCrawlerException("Error writing to buffer", e, true);
            }
        }
    }
}
