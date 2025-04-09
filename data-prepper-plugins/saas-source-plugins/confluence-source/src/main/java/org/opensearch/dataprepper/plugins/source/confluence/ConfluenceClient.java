/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.confluence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventType;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.confluence.utils.HtmlToTextConversionUtil;
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
import static org.opensearch.dataprepper.plugins.source.confluence.utils.Constants.SPACE;

/**
 * This class represents a Confluence client.
 */
@Named
public class ConfluenceClient implements CrawlerClient {

    private static final Logger log = LoggerFactory.getLogger(ConfluenceClient.class);
    private ObjectMapper objectMapper = new ObjectMapper();
    private final ConfluenceService service;
    private final ConfluenceIterator confluenceIterator;
    private final ExecutorService executorService;
    private final CrawlerSourceConfig configuration;
    private final int bufferWriteTimeoutInSeconds = 10;

    public ConfluenceClient(ConfluenceService service,
                            ConfluenceIterator confluenceIterator,
                            PluginExecutorServiceProvider executorServiceProvider,
                            ConfluenceSourceConfig sourceConfig) {
        this.service = service;
        this.confluenceIterator = confluenceIterator;
        this.executorService = executorServiceProvider.get();
        this.configuration = sourceConfig;
    }

    @Override
    public Iterator<ItemInfo> listItems(Instant lastPollTime) {
        confluenceIterator.initialize(lastPollTime);
        return confluenceIterator;
    }

    @VisibleForTesting
    public void injectObjectMapper(ObjectMapper objectMapper) {
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
        String space = (String) keyAttributes.get(SPACE);
        Instant eventTime = state.getExportStartTime();
        List<ItemInfo> itemInfos = new ArrayList<>();
        for (String itemId : itemIds) {
            if (itemId == null) {
                continue;
            }
            ItemInfo itemInfo = ConfluenceItemInfo.builder()
                    .withItemId(itemId)
                    .withId(itemId)
                    .withSpace(space)
                    .withEventTime(eventTime)
                    .withMetadata(keyAttributes).build();
            itemInfos.add(itemInfo);
        }

        String eventType = EventType.DOCUMENT.toString();
        List<Record<Event>> recordsToWrite = itemInfos
                .parallelStream()
                .map(t -> (Supplier<String>) (() -> service.getContent(t.getId())))
                .map(supplier -> supplyAsync(supplier, this.executorService))
                .map(CompletableFuture::join)
                .map(contentJson -> {
                    try {
                        ObjectNode contentJsonObj = objectMapper.readValue(contentJson, new TypeReference<>() {
                        });
                        return HtmlToTextConversionUtil.convertHtmlToText(contentJsonObj, "body/view/value");
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
