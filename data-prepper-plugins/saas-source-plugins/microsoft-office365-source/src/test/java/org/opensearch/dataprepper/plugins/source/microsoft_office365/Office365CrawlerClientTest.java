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
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.PluginExecutorServiceProvider;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.PaginationCrawlerWorkerProgressState;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.service.Office365Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class Office365CrawlerClientTest {
    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private PaginationCrawlerWorkerProgressState state;

    @Mock
    private AcknowledgementSet acknowledgementSet;

    @Mock
    private Office365SourceConfig sourceConfig;

    @Mock
    private Office365Service service;

    @Mock
    private Office365Iterator office365Iterator;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Timer bufferWriteLatencyTimer;

    @Mock
    private static Logger log;

    private final PluginExecutorServiceProvider executorServiceProvider = new PluginExecutorServiceProvider();

    private static final String BUFFER_WRITE_FAILURES_TIMEOUT = "bufferWriteFailuresTimeout";

    @BeforeAll
    static void setupLogger() {
        log = mock(Logger.class);
        LoggerFactory.getLogger(Office365CrawlerClient.class).info("Mocking the logger");
    }

    @BeforeEach
    void setUp() {
        when(pluginMetrics.timer(anyString())).thenReturn(bufferWriteLatencyTimer);
        when(pluginMetrics.counter(anyString())).thenReturn(mock(Counter.class));
    }

    @Test
    void testConstructor() {
        Office365CrawlerClient client = new Office365CrawlerClient(service, office365Iterator, executorServiceProvider, sourceConfig, pluginMetrics);
        assertNotNull(client);
    }

    @Test
    void testListItems() {
        Office365CrawlerClient client = new Office365CrawlerClient(service, office365Iterator, executorServiceProvider, sourceConfig, pluginMetrics);
        assertNotNull(client.listItems(Instant.ofEpochSecond(1234L)));
        verify(service).initializeSubscriptions();
        verify(office365Iterator).initialize(any(Instant.class));
    }

    @Test
    void testExecutePartition() throws Exception {
        Office365CrawlerClient client = new Office365CrawlerClient(service, office365Iterator, executorServiceProvider, sourceConfig, pluginMetrics);

        List<String> itemIds = Arrays.asList("ID1", "ID2");
        when(state.getItemIds()).thenReturn(itemIds);
        when(service.getAuditLog(anyString())).thenReturn("{\"Workload\":\"Exchange\",\"Operation\":\"Test\"}");

        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(bufferWriteLatencyTimer).record(any(Runnable.class));

        ArgumentCaptor<Collection<Record<Event>>> recordsCaptor = ArgumentCaptor.forClass((Class) Collection.class);

        client.executePartition(state, buffer, acknowledgementSet);

        verify(buffer).writeAll(recordsCaptor.capture(), anyInt());
        Collection<Record<Event>> capturedRecords = recordsCaptor.getValue();

        assertFalse(capturedRecords.isEmpty());
        assertEquals(itemIds.size(), capturedRecords.size());
        for (Record<Event> record : capturedRecords) {
            assertNotNull(record.getData());
            assertEquals("Exchange", record.getData().getMetadata().getAttribute("contentType"));
        }
    }

    @Test
    void testExecutePartitionWithJsonProcessingError() throws Exception {
        Office365CrawlerClient client = new Office365CrawlerClient(service, office365Iterator, executorServiceProvider, sourceConfig, pluginMetrics);
        ObjectMapper mockObjectMapper = mock(ObjectMapper.class);
        client.injectObjectMapper(mockObjectMapper);

        List<String> itemIds = List.of("ID1");
        when(state.getItemIds()).thenReturn(itemIds);
        when(service.getAuditLog(anyString())).thenReturn("{\"invalid\":json}");

        when(mockObjectMapper.readTree(anyString())).thenThrow(new JsonProcessingException("Test error") {});

        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(bufferWriteLatencyTimer).record(any(Runnable.class));

        client.executePartition(state, buffer, acknowledgementSet);

        verify(buffer).writeAll(argThat(list -> list.isEmpty()), anyInt());
    }

    @Test
    void testBufferWriteWithAcknowledgements() throws Exception {
        Office365CrawlerClient client = new Office365CrawlerClient(service, office365Iterator, executorServiceProvider, sourceConfig, pluginMetrics);

        List<String> itemIds = List.of("ID1");
        when(state.getItemIds()).thenReturn(itemIds);
        when(service.getAuditLog(anyString())).thenReturn("{\"Workload\":\"Exchange\",\"Operation\":\"Test\"}");
        when(sourceConfig.isAcknowledgments()).thenReturn(true);

        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(bufferWriteLatencyTimer).record(any(Runnable.class));

        client.executePartition(state, buffer, acknowledgementSet);

        verify(acknowledgementSet).add(any(Event.class));
        verify(acknowledgementSet).complete();
        verify(buffer).writeAll(any(), anyInt());
    }

    @Test
    void testBufferWriteTimeout() throws Exception {
        Office365CrawlerClient client = new Office365CrawlerClient(service, office365Iterator, executorServiceProvider, sourceConfig, pluginMetrics);

        List<String> itemIds = List.of("ID1");
        when(state.getItemIds()).thenReturn(itemIds);
        when(service.getAuditLog(anyString())).thenReturn("{\"Workload\":\"Exchange\",\"Operation\":\"Test\"}");

        // Mock the timer to execute the runnable
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(bufferWriteLatencyTimer).record(any(Runnable.class));

        // Configure buffer to throw RuntimeException directly
        doThrow(new RuntimeException("Error writing to buffer"))
                .when(buffer)
                .writeAll(any(), anyInt());

        // Execute and verify exception
        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> client.executePartition(state, buffer, acknowledgementSet));

        // Verify the exact error message
        assertEquals("Error writing to buffer", exception.getMessage());

        // Verify that buffer.writeAll was called exactly once
        verify(buffer).writeAll(any(), anyInt());
    }

    @Test
    void testNonRetryableError() throws Exception {
        Office365CrawlerClient client = new Office365CrawlerClient(service, office365Iterator, executorServiceProvider, sourceConfig, pluginMetrics);

        List<String> itemIds = List.of("ID1");
        when(state.getItemIds()).thenReturn(itemIds);
        when(service.getAuditLog(anyString())).thenReturn(null);

        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(bufferWriteLatencyTimer).record(any(Runnable.class));

        client.executePartition(state, buffer, acknowledgementSet);

        verify(buffer).writeAll(argThat(list -> list.isEmpty()), anyInt());
    }

    @Test
    void testMissingWorkloadField() throws Exception {
        Office365CrawlerClient client = new Office365CrawlerClient(service, office365Iterator, executorServiceProvider, sourceConfig, pluginMetrics);

        List<String> itemIds = List.of("ID1");
        when(state.getItemIds()).thenReturn(itemIds);
        when(service.getAuditLog(anyString())).thenReturn("{\"Operation\":\"Test\"}");

        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(bufferWriteLatencyTimer).record(any(Runnable.class));

        client.executePartition(state, buffer, acknowledgementSet);

        verify(buffer).writeAll(argThat(list -> list.isEmpty()), anyInt());
    }
}
