/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.office365;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventType;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.CrawlerClient;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.PluginExecutorServiceProvider;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.SaasWorkerProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.ItemInfo;

import static org.opensearch.dataprepper.plugins.source.office365.configuration.MetadataKeyAttributes.CONTENT_TYPE;
import javax.inject.Named;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * Implementation of CrawlerClient for Office 365 audit logs.
 * This class manages the crawling process for Office 365 audit logs.
 */
@Slf4j
@Named
public class Office365CrawlerClient implements CrawlerClient {
    private static final int BUFFER_TIMEOUT_IN_SECONDS = 10;

    private final Office365Service service;
    private final Office365Iterator office365Iterator;
    private final ExecutorService executorService;
    private final Office365SourceConfig configuration;
    private final ObjectMapper objectMapper;

    public Office365CrawlerClient(final Office365Service service,
                                  final Office365Iterator office365Iterator,
                                  final PluginExecutorServiceProvider executorServiceProvider,
                                  final Office365SourceConfig sourceConfig) {
        this.service = service;
        this.office365Iterator = office365Iterator;
        this.executorService = executorServiceProvider.get();
        this.configuration = sourceConfig;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public Iterator<ItemInfo> listItems(final Instant lastPollTime) {
        log.info("Starting to list Office 365 audit logs from {}", lastPollTime);
        // TODO: Consider moving to a SubscriptionService later
        // Initialize subscription every time in the LP to ensure there hasn't been a subscription change
        service.initializeSubscriptions();
        office365Iterator.initialize(lastPollTime);
        return office365Iterator;
    }

    @Override
    public void executePartition(final SaasWorkerProgressState state,
                                 final Buffer<Record<Event>> buffer,
                                 final AcknowledgementSet acknowledgementSet) {
        log.info("Executing partition: {} with {} log(s)", state.getKeyAttributes(), state.getItemIds().size());
        List<String> itemIds = state.getItemIds();
        Map<String, Object> keyAttributes = state.getKeyAttributes();
        String contentType = (String) keyAttributes.get("type");

        List<Record<Event>> records = itemIds.stream()
                .map(id -> {
                    try {
                        String auditLog = service.getAuditLog(id); // fetch each individual log
                        JsonNode jsonNode = objectMapper.readTree(auditLog);
                        Map<String, Object> data;

                        if (jsonNode.isArray() && !jsonNode.isEmpty()) {
                            data = objectMapper.convertValue(jsonNode.get(0), new TypeReference<Map<String, Object>>() {});
                        } else {
                            data = objectMapper.readValue(auditLog, new TypeReference<Map<String, Object>>() {});
                        }

                        Event event = JacksonEvent.builder()
                                .withEventType(EventType.LOG.toString())
                                .withData(data)
                                .build();
                        event.getMetadata().setAttribute(CONTENT_TYPE, contentType);
                        return new Record<>(event);
                    } catch (Exception e) {
                        // TODO: Handle failed retrievals here so we don't drop records
                        log.error("Error processing audit log entry for ID: {}", id, e);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        try {
            log.info("Writing {} records to buffer", records.size());
            int timeoutMillis = (int) Duration.ofSeconds(BUFFER_TIMEOUT_IN_SECONDS).toMillis();

            if (configuration.isAcknowledgments()) {
                records.forEach(record -> acknowledgementSet.add(record.getData()));
                buffer.writeAll(records, timeoutMillis);
                acknowledgementSet.complete();
            } else {
                buffer.writeAll(records, timeoutMillis);
            }
        } catch (Exception e) {
            log.error("Failed to write records to buffer", e);
            throw new RuntimeException("Failed to write records to buffer", e);
        }
    }
}
