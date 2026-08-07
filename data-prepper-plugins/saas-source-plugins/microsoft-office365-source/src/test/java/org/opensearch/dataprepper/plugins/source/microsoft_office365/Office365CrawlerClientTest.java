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
import org.opensearch.dataprepper.plugins.source.source_crawler.exception.SaaSCrawlerException;
import org.opensearch.dataprepper.plugins.source.source_crawler.metrics.VendorAPIMetricsRecorder;
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
import static org.junit.jupiter.api.Assertions.assertTrue;
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
import static org.mockito.Mockito.never;

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
    private VendorAPIMetricsRecorder metricsRecorder;

    @Mock
    private static Logger log;

    @BeforeAll
    static void setupLogger() {
        log = mock(Logger.class);
        LoggerFactory.getLogger(Office365CrawlerClient.class).info("Mocking the logger");
    }

    @BeforeEach
    void setUp() {
        when(state.getStartTime()).thenReturn(Instant.now().minus(Duration.ofHours(1)));
        when(state.getEndTime()).thenReturn(Instant.now());
        when(state.getDimensionType()).thenReturn("Exchange");
    }

    @Test
    void testConstructor() {
        Office365CrawlerClient client = new Office365CrawlerClient(service, sourceConfig, metricsRecorder);
        assertNotNull(client);
    }

    @Test
    void testExecutePartition() throws Exception {
        Office365CrawlerClient client = new Office365CrawlerClient(service, sourceConfig, metricsRecorder);

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

        // Mock the metrics recorder methods
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(metricsRecorder).recordBufferWriteLatency(any(Runnable.class));

        ArgumentCaptor<Collection<Record<Event>>> recordsCaptor = ArgumentCaptor.forClass((Class) Collection.class);

        client.executePartition(state, buffer, acknowledgementSet);

        verify(buffer).writeAll(recordsCaptor.capture(), anyInt());
        Collection<Record<Event>> capturedRecords = recordsCaptor.getValue();

        assertFalse(capturedRecords.isEmpty());
        assertEquals(1, capturedRecords.size());
        for (Record<Event> record : capturedRecords) {
            assertNotNull(record.getData());
            assertEquals("Exchange", record.getData().getMetadata().getAttribute("contentType"));
        }

        verify(metricsRecorder).recordBufferWriteLatency(any(Runnable.class));
        verify(metricsRecorder).recordBufferWriteAttempt();
        verify(metricsRecorder).recordBufferWriteSuccess();
    }

    @Test
    void testExecutePartitionWithJsonProcessingError() throws Exception {
        Office365CrawlerClient client = new Office365CrawlerClient(service, sourceConfig, metricsRecorder);
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
        }).when(metricsRecorder).recordBufferWriteLatency(any(Runnable.class));

        SaaSCrawlerException exception = assertThrows(SaaSCrawlerException.class,
            () -> client.executePartition(state, buffer, acknowledgementSet));

        assertEquals("Error processing audit log: ID1", exception.getMessage());
        assertFalse(exception.isRetryable());
        assertTrue(exception.getCause() instanceof SaaSCrawlerException);
        assertEquals("Failed to parse audit log: ID1", exception.getCause().getMessage());

        verify(metricsRecorder).recordError(any(Exception.class));
        verify(metricsRecorder).recordNonRetryableError();
    }

    @Test
    void testBufferWriteWithAcknowledgements() throws Exception {
        Office365CrawlerClient client = new Office365CrawlerClient(service, sourceConfig, metricsRecorder);

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
        }).when(metricsRecorder).recordBufferWriteLatency(any(Runnable.class));

        client.executePartition(state, buffer, acknowledgementSet);

        verify(acknowledgementSet).add(any(Event.class));
        verify(acknowledgementSet).complete();
        verify(buffer).writeAll(any(), anyInt());
        verify(metricsRecorder).recordBufferWriteLatency(any(Runnable.class));
    }

    @Test
    void testBufferWriteTimeout() throws Exception {
        Office365CrawlerClient client = new Office365CrawlerClient(service, sourceConfig, metricsRecorder);

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
        }).when(metricsRecorder).recordBufferWriteLatency(any(Runnable.class));

        doThrow(new RuntimeException("Error writing to buffer"))
                .when(buffer)
                .writeAll(any(), anyInt());

        SaaSCrawlerException exception = assertThrows(SaaSCrawlerException.class,
                () -> client.executePartition(state, buffer, acknowledgementSet));

        assertEquals("Error writing to buffer", exception.getMessage());
        assertTrue(exception.isRetryable());
        verify(buffer).writeAll(any(), anyInt());
        verify(metricsRecorder).recordBufferWriteFailure();
    }

    @Test
    void testNonRetryableError() throws Exception {
        Office365CrawlerClient client = new Office365CrawlerClient(service, sourceConfig, metricsRecorder);

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
        }).when(metricsRecorder).recordBufferWriteLatency(any(Runnable.class));

        SaaSCrawlerException exception = assertThrows(SaaSCrawlerException.class,
            () -> client.executePartition(state, buffer, acknowledgementSet));

        assertEquals("Error processing audit log: ID1", exception.getMessage());
        assertFalse(exception.isRetryable());
        assertTrue(exception.getCause() instanceof SaaSCrawlerException);
        assertEquals("Received null log content for URI: uri1", exception.getCause().getMessage());

        verify(buffer, never()).writeAll(argThat(list -> list.isEmpty()), anyInt());
        verify(metricsRecorder).recordError(any(Exception.class));
        verify(metricsRecorder).recordNonRetryableError();
    }

    @Test
    void testRetryableErrorCounterIncrement() throws Exception {
        Office365CrawlerClient client = new Office365CrawlerClient(service, sourceConfig, metricsRecorder);

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

        // Mock service.getAuditLog to throw a retryable SaaSCrawlerException
        when(service.getAuditLog(anyString()))
                .thenThrow(new SaaSCrawlerException("Retryable error", true));

        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(metricsRecorder).recordBufferWriteLatency(any(Runnable.class));

        // Execute and expect RuntimeException to be thrown due to retryable error
        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> client.executePartition(state, buffer, acknowledgementSet));

        // Verify retryable error counter was incremented
        verify(metricsRecorder).recordError(any(Exception.class));
        verify(metricsRecorder).recordRetryableError();
        assertEquals("Error processing audit log: ID1", exception.getMessage());
    }

    @Test
    void testMissingWorkloadField() throws Exception {
        Office365CrawlerClient client = new Office365CrawlerClient(service, sourceConfig, metricsRecorder);

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
        }).when(metricsRecorder).recordBufferWriteLatency(any(Runnable.class));

        SaaSCrawlerException exception = assertThrows(SaaSCrawlerException.class,
            () -> client.executePartition(state, buffer, acknowledgementSet));

        assertEquals("Error processing audit log: ID1", exception.getMessage());
        assertFalse(exception.isRetryable());
        assertTrue(exception.getCause() instanceof SaaSCrawlerException);
        assertEquals("Missing Workload field in audit log: ID1", exception.getCause().getMessage());

        verify(buffer, never()).writeAll(argThat(list -> list.isEmpty()), anyInt());
        verify(metricsRecorder).recordError(any(Exception.class));
        verify(metricsRecorder).recordNonRetryableError();
    }

    @Test
    void testExecutePartitionWithSearchAuditLogsError() throws Exception {
        Office365CrawlerClient client = new Office365CrawlerClient(service, sourceConfig, metricsRecorder);

        // Mock searchAuditLogs to throw exception
        when(service.searchAuditLogs(
                eq("Exchange"),  // Match the value from setUp()
                any(Instant.class),
                any(Instant.class),
                isNull()
        )).thenThrow(new SaaSCrawlerException("Search audit logs failed", true));

        // Execute and verify exception
        SaaSCrawlerException exception = assertThrows(SaaSCrawlerException.class,
                () -> client.executePartition(state, buffer, acknowledgementSet));

        // Verify exception message and counter increment
        assertEquals("Search audit logs failed", exception.getMessage());
        assertTrue(exception.isRetryable());
        verify(metricsRecorder).recordError(any(Exception.class));
        verify(metricsRecorder).recordRetryableError();
    }

    @Test
    void testExecutePartitionWithNonSaaSCrawlerException() throws Exception {
        Office365CrawlerClient client = new Office365CrawlerClient(service, sourceConfig, metricsRecorder);

        // Simulate a non-SaaSCrawlerException (like RuntimeException)
        when(service.searchAuditLogs(
                anyString(),
                any(Instant.class),
                any(Instant.class),
                any()
        )).thenThrow(new RuntimeException("Unexpected error"));

        // Execute and verify exception
        SaaSCrawlerException exception = assertThrows(SaaSCrawlerException.class,
                () -> client.executePartition(state, buffer, acknowledgementSet));

        // Verify:
        assertEquals("Failed to process partition", exception.getMessage());
        assertFalse(exception.isRetryable());
        assertTrue(exception.getCause() instanceof RuntimeException);
        verify(metricsRecorder).recordError(any(Exception.class));
        verify(metricsRecorder).recordNonRetryableError();
    }

    @Test
    void testBufferWriteRetrySuccess() throws Exception {
        Office365CrawlerClient client = new Office365CrawlerClient(service, sourceConfig, metricsRecorder);

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
        }).when(metricsRecorder).recordBufferWriteLatency(any(Runnable.class));

        client.executePartition(state, buffer, acknowledgementSet);

        verify(metricsRecorder).recordBufferWriteAttempt();
        verify(metricsRecorder).recordBufferWriteSuccess();
        verify(metricsRecorder, never()).recordBufferWriteRetrySuccess();
        verify(metricsRecorder, never()).recordBufferWriteFailure();
    }
}
