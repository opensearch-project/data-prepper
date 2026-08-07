/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.pull_ingestion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.plugins.sink.opensearch.DocumentIdResolver;
import org.opensearch.dataprepper.plugins.sink.opensearch.EventActionResolver;
import org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.opensearch.PullEngine;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConfiguration;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PullIngesterTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Mock
    private PullEngine pullEngine;

    @Mock
    private IndexRouter indexRouter;

    @Mock
    private IndexShardProvider indexShardProvider;

    @Mock
    private PullIngestionEnvelopeBuilder envelopeBuilder;

    @Mock
    private DocumentIdResolver documentIdResolver;

    @Mock
    private OpenSearchSinkConfiguration openSearchSinkConfig;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    @Mock
    private SinkContext sinkContext;

    @Mock
    private EventActionResolver eventActionResolver;

    @Mock
    private IndexConfiguration indexConfiguration;

    @Mock
    private PullIngestionMetrics pullIngestionMetrics;

    private String indexAlias;
    private String versionExpression;
    private String ingestionTopic;
    private int shardCount;

    @BeforeEach
    void setUp() {
        indexAlias = UUID.randomUUID().toString();
        versionExpression = "${/" + UUID.randomUUID().toString() + "}";
        ingestionTopic = UUID.randomUUID().toString();
        shardCount = 5;

        when(openSearchSinkConfig.getIndexConfiguration()).thenReturn(indexConfiguration);
        lenient().when(indexConfiguration.getVersionExpression()).thenReturn(versionExpression);
        lenient().when(indexConfiguration.getDocumentRootKey()).thenReturn(null);
        lenient().when(indexConfiguration.getIndexAlias()).thenReturn(indexAlias);
        lenient().when(indexConfiguration.getRoutingField()).thenReturn(null);
        lenient().when(indexConfiguration.getRouting()).thenReturn(null);

        lenient().when(sinkContext.getTagsTargetKey()).thenReturn(null);
        lenient().when(sinkContext.getIncludeKeys()).thenReturn(null);
        lenient().when(sinkContext.getExcludeKeys()).thenReturn(null);

        lenient().doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(pullIngestionMetrics).recordLatency(any(Runnable.class));
    }

    private PullIngester createObjectUnderTest() {
        return new PullIngester(pullEngine, indexRouter, indexShardProvider, envelopeBuilder, documentIdResolver,
                openSearchSinkConfig, expressionEvaluator, sinkContext, eventActionResolver, pullIngestionMetrics);
    }

    @Test
    void initialize_initializes_index_router_and_pull_engine() throws IOException {
        when(indexRouter.getNumberOfShards()).thenReturn(shardCount);
        when(indexShardProvider.getIngestionTopic(indexAlias)).thenReturn(ingestionTopic);

        createObjectUnderTest().initialize();

        verify(indexRouter).initialize(indexAlias);
        verify(pullEngine).initialize(ingestionTopic, shardCount);
    }

    @Test
    void output_writes_event_to_correct_partition() throws Exception {
        final String docId = UUID.randomUUID().toString();
        final String versionValue = "1000";
        final String action = UUID.randomUUID().toString();
        final int partition = 3;
        final Event event = createEvent(Map.of("field", UUID.randomUUID().toString()));
        final byte[] envelopeBytes = UUID.randomUUID().toString().getBytes();

        when(documentIdResolver.resolve(event)).thenReturn(Optional.of(docId));
        when(event.formatString(versionExpression, expressionEvaluator)).thenReturn(versionValue);
        when(eventActionResolver.resolveAction(event, indexAlias)).thenReturn(action);
        when(envelopeBuilder.build(eq(docId), eq(1000L), anyString(), eq(action))).thenReturn(envelopeBytes);
        when(indexRouter.getShardForRouting(docId)).thenReturn(partition);

        createObjectUnderTest().output(List.of(new Record<>(event)));

        verify(pullEngine).write(partition, docId, envelopeBytes);
        verify(pullEngine).flush();
        verify(event.getEventHandle()).release(true);
        verify(pullIngestionMetrics).incrementDocumentsSucceeded();
        verify(pullIngestionMetrics).recordBytes(envelopeBytes.length);
        verify(pullIngestionMetrics).recordLatency(any(Runnable.class));
    }

    @Test
    void output_releases_event_handle_false_on_failure() {
        final Event event = createEvent(Map.of("field", UUID.randomUUID().toString()));

        when(documentIdResolver.resolve(event)).thenReturn(Optional.empty());

        createObjectUnderTest().output(List.of(new Record<>(event)));

        verify(pullEngine, never()).write(anyInt(), anyString(), any(byte[].class));
        verify(pullEngine).flush();
        verify(event.getEventHandle()).release(false);
        verify(pullIngestionMetrics).incrementDocumentsFailed();
        verify(pullIngestionMetrics, never()).incrementDocumentsSucceeded();
    }

    @Test
    void output_uses_routing_field_when_configured() throws Exception {
        final String routingField = UUID.randomUUID().toString();
        final String routingValue = UUID.randomUUID().toString();
        final String docId = UUID.randomUUID().toString();
        final int partition = 2;

        when(indexConfiguration.getRoutingField()).thenReturn(routingField);

        final Event event = createEvent(Map.of("field", UUID.randomUUID().toString()));

        when(documentIdResolver.resolve(event)).thenReturn(Optional.of(docId));
        when(event.formatString(versionExpression, expressionEvaluator)).thenReturn("1");
        when(event.get(routingField, String.class)).thenReturn(routingValue);
        when(eventActionResolver.resolveAction(event, indexAlias)).thenReturn("index");
        when(envelopeBuilder.build(eq(docId), eq(1L), anyString(), eq("index"))).thenReturn(new byte[0]);
        when(indexRouter.getShardForRouting(routingValue)).thenReturn(partition);

        createObjectUnderTest().output(List.of(new Record<>(event)));

        verify(indexRouter).getShardForRouting(routingValue);
        verify(pullEngine).write(eq(partition), eq(docId), any(byte[].class));
    }

    @Test
    void shutdown_delegates_to_pull_engine() {
        createObjectUnderTest().shutdown();
        verify(pullEngine).shutdown();
    }

    @Test
    void output_with_empty_records_only_flushes() {
        createObjectUnderTest().output(Collections.emptyList());

        verify(pullEngine, never()).write(anyInt(), anyString(), any(byte[].class));
        verify(pullEngine).flush();
    }

    private Event createEvent(final Map<String, Object> data) {
        final Event event = mock(Event.class);
        final EventHandle eventHandle = mock(EventHandle.class);
        lenient().when(event.getEventHandle()).thenReturn(eventHandle);
        lenient().when(event.toJsonString()).thenReturn(toJson(data));

        lenient().when(event.jsonBuilder()).thenReturn(JacksonEvent.builder()
                .withEventType("event")
                .withData(data)
                .build()
                .jsonBuilder());

        return event;
    }

    private String toJson(final Map<String, Object> data) {
        try {
            return OBJECT_MAPPER.writeValueAsString(data);
        } catch (final JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
