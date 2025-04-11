/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.jira;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventType;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.CrawlerClient;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.CrawlerSourceConfig;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.PluginExecutorServiceProvider;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.SaasWorkerProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.ItemInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.PROJECT;

/**
 * This class represents a Jira client.
 */
@Named
public class JiraClient implements CrawlerClient {

    private static final Logger log = LoggerFactory.getLogger(JiraClient.class);
    private ObjectMapper objectMapper = new ObjectMapper();
    private final JiraService service;
    private final JiraIterator jiraIterator;
    private final ExecutorService executorService;
    private final CrawlerSourceConfig configuration;
    private final int bufferWriteTimeoutInSeconds = 10;

    public JiraClient(JiraService service,
                      JiraIterator jiraIterator,
                      PluginExecutorServiceProvider executorServiceProvider,
                      JiraSourceConfig sourceConfig) {
        this.service = service;
        this.jiraIterator = jiraIterator;
        this.executorService = executorServiceProvider.get();
        this.configuration = sourceConfig;
    }

    @Override
    public Iterator<ItemInfo> listItems(Instant lastPollTime) {
        jiraIterator.initialize(lastPollTime);
        return jiraIterator;
    }

    @VisibleForTesting
    void injectObjectMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public void executePartition(SaasWorkerProgressState state,
                                 Buffer<Record<Event>> buffer,
                                 AcknowledgementSet acknowledgementSet) {
        log.trace("Executing the partition: {} with {} ticket(s)",
                state.getKeyAttributes(), state.getItemIds().size());
        List<String> itemIds = state.getItemIds();
        Map<String, Object> keyAttributes = state.getKeyAttributes();
        String project = (String) keyAttributes.get(PROJECT);
        Instant eventTime = state.getExportStartTime();
        List<ItemInfo> itemInfos = new ArrayList<>();
        for (String itemId : itemIds) {
            if (itemId == null) {
                continue;
            }
            ItemInfo itemInfo = JiraItemInfo.builder()
                    .withItemId(itemId)
                    .withId(itemId)
                    .withProject(project)
                    .withEventTime(eventTime)
                    .withMetadata(keyAttributes).build();
            itemInfos.add(itemInfo);
        }

        String eventType = EventType.DOCUMENT.toString();
        List<Record<Event>> recordsToWrite = itemInfos
                .parallelStream()
                .map(t -> (Supplier<String>) (() -> service.getIssue(t.getId())))
                .map(supplier -> supplyAsync(supplier, this.executorService))
                .map(CompletableFuture::join)
                .map(ticketJson -> {
                    try {
                        return objectMapper.readValue(ticketJson, new TypeReference<>() {
                        });
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                })
                .map(t -> (Event) JacksonEvent.builder()
                        .withEventType(eventType)
                        .withData(t)
                        .build())
                .map(Record::new)
                .collect(Collectors.toList());

        try {
            if (configuration.isAcknowledgments()) {
                recordsToWrite.forEach(eventRecord -> acknowledgementSet.add(eventRecord.getData()));
                buffer.writeAll(recordsToWrite, (int) Duration.ofSeconds(bufferWriteTimeoutInSeconds).toMillis());
                acknowledgementSet.complete();
            } else {
                buffer.writeAll(recordsToWrite, (int) Duration.ofSeconds(bufferWriteTimeoutInSeconds).toMillis());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
