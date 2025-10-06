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
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.models.AuditLogsResponse;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.DimensionalTimeSliceWorkerProgressState;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.service.Office365Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class Office365CrawlerClientTest {
    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private DimensionalTimeSliceWorkerProgressState state;

    @Mock
    private AcknowledgementSet acknowledgementSet;

    @Mock
    private Office365SourceConfig sourceConfig;

    @Mock
    private Office365Service service;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Timer bufferWriteLatencyTimer;

    @Mock
    private static Logger log;

    @BeforeAll
    static void setupLogger() {
        log = mock(Logger.class);
        LoggerFactory.getLogger(Office365CrawlerClient.class).info("Mocking the logger");
    }

    @BeforeEach
    void setUp() {
        when(pluginMetrics.timer(anyString())).thenReturn(bufferWriteLatencyTimer);
        when(pluginMetrics.counter(anyString())).thenReturn(mock(Counter.class));
        when(state.getStartTime()).thenReturn(Instant.now().minus(Duration.ofHours(1)));
        when(state.getEndTime()).thenReturn(Instant.now());
        when(state.getDimensionType()).thenReturn("Exchange");
    }

    @Test
    void testConstructor() {
        Office365CrawlerClient client = new Office365CrawlerClient(service, sourceConfig, pluginMetrics);
        assertNotNull(client);
    }

    @Test
    void testExecutePartition() throws Exception {
        Office365CrawlerClient client = new Office365CrawlerClient(service, sourceConfig, pluginMetrics);

        AuditLogsResponse response = new AuditLogsResponse(
                Arrays.asList(Map.of(
                        "contentId", "ID1",
                        "contentUri", "uri1"
                )), null);

        when(service.searchAuditLogs(
                eq("Exchange"),
                any(Instant.class),
                any(Instant.class),
                isNull()
        )).thenReturn(response);

        when(service.getAuditLog(anyString()))
                .thenReturn("{\"Workload\":\"Exchange\",\"Operation\":\"Test\"}");

        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(bufferWriteLatencyTimer).record(any(Runnable.class));

        ArgumentCaptor<Record<Event>> recordCaptor = ArgumentCaptor.forClass(Record.class);

        client.executePartition(state, buffer, acknowledgementSet);

        verify(buffer).write(recordCaptor.capture(), anyInt());
        Record<Event> capturedRecord = recordCaptor.getValue();

        assertNotNull(capturedRecord.getData());
        assertEquals("Exchange", capturedRecord.getData().getMetadata().getAttribute("contentType"));
    }

    @Test
    void testExecutePartitionWithJsonProcessingError() throws Exception {
        Office365CrawlerClient client = new Office365CrawlerClient(service, sourceConfig, pluginMetrics);
        ObjectMapper mockObjectMapper = mock(ObjectMapper.class);
        client.injectObjectMapper(mockObjectMapper);

        AuditLogsResponse response = new AuditLogsResponse(
                Arrays.asList(Map.of(
                        "contentId", "ID1",
                        "contentUri", "uri1"
                )), null);

        when(service.searchAuditLogs(
                anyString(),
                any(Instant.class),
                any(Instant.class),
                any()
        )).thenReturn(response);

        when(service.getAuditLog(anyString())).thenReturn("{\"invalid\":json}");
        when(mockObjectMapper.readTree(anyString())).thenThrow(new JsonProcessingException("Test error") {});

        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(bufferWriteLatencyTimer).record(any(Runnable.class));

        client.executePartition(state, buffer, acknowledgementSet);

        // With individual record processing, no records should be written due to JSON processing error
        verify(buffer, org.mockito.Mockito.never()).write(any(Record.class), anyInt());
    }

    @Test
    void testBufferWriteWithAcknowledgements() throws Exception {
        Office365CrawlerClient client = new Office365CrawlerClient(service, sourceConfig, pluginMetrics);

        AuditLogsResponse response = new AuditLogsResponse(
                Arrays.asList(Map.of(
                        "contentId", "ID1",
                        "contentUri", "uri1"
                )), null);

        when(service.searchAuditLogs(
                anyString(),
                any(Instant.class),
                any(Instant.class),
                any()
        )).thenReturn(response);

        when(service.getAuditLog(anyString()))
                .thenReturn("{\"Workload\":\"Exchange\",\"Operation\":\"Test\"}");
        when(sourceConfig.isAcknowledgments()).thenReturn(true);

        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(bufferWriteLatencyTimer).record(any(Runnable.class));

        client.executePartition(state, buffer, acknowledgementSet);

        verify(acknowledgementSet).add(any(Event.class));
        verify(acknowledgementSet).complete();
        verify(buffer).write(any(Record.class), anyInt());
    }

    @Test
    void testBufferWriteTimeout() throws Exception {
        Office365CrawlerClient client = new Office365CrawlerClient(service, sourceConfig, pluginMetrics);

        AuditLogsResponse response = new AuditLogsResponse(
                Arrays.asList(Map.of(
                        "contentId", "ID1",
                        "contentUri", "uri1"
                )), null);

        when(service.searchAuditLogs(
                anyString(),
                any(Instant.class),
                any(Instant.class),
                any()
        )).thenReturn(response);

        when(service.getAuditLog(anyString()))
                .thenReturn("{\"Workload\":\"Exchange\",\"Operation\":\"Test\"}");

        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(bufferWriteLatencyTimer).record(any(Runnable.class));

        doThrow(new RuntimeException("Error writing to buffer"))
                .when(buffer)
                .write(any(Record.class), anyInt());

        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> client.executePartition(state, buffer, acknowledgementSet));

        // With individual record processing, the exception gets wrapped with the log ID
        assertEquals("Unexpected error processing audit log: ID1", exception.getMessage());
        verify(buffer).write(any(Record.class), anyInt());
    }

    @Test
    void testNonRetryableError() throws Exception {
        Office365CrawlerClient client = new Office365CrawlerClient(service, sourceConfig, pluginMetrics);

        AuditLogsResponse response = new AuditLogsResponse(
                Arrays.asList(Map.of(
                        "contentId", "ID1",
                        "contentUri", "uri1")), null);

        when(service.searchAuditLogs(
                anyString(),
                any(Instant.class),
                any(Instant.class),
                any()
        )).thenReturn(response);

        when(service.getAuditLog(anyString())).thenReturn(null);

        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(bufferWriteLatencyTimer).record(any(Runnable.class));

        client.executePartition(state, buffer, acknowledgementSet);

        // With individual record processing, no records should be written due to null content
        verify(buffer, org.mockito.Mockito.never()).write(any(Record.class), anyInt());
    }

    @Test
    void testMissingWorkloadField() throws Exception {
        Office365CrawlerClient client = new Office365CrawlerClient(service, sourceConfig, pluginMetrics);

        AuditLogsResponse response = new AuditLogsResponse(
                Arrays.asList(Map.of(
                        "contentId", "ID1",
                        "contentUri", "uri1"
                )), null);

        when(service.searchAuditLogs(
                anyString(),
                any(Instant.class),
                any(Instant.class),
                any()
        )).thenReturn(response);

        when(service.getAuditLog(anyString())).thenReturn("{\"Operation\":\"Test\"}");

        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(bufferWriteLatencyTimer).record(any(Runnable.class));

        client.executePartition(state, buffer, acknowledgementSet);

        // With individual record processing, no records should be written due to missing Workload field
        verify(buffer, org.mockito.Mockito.never()).write(any(Record.class), anyInt());
    }
}
