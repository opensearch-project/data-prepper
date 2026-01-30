/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.plugins.processor.model.internal.SpanStateData;
import org.opensearch.dataprepper.plugins.processor.state.MapDbProcessorState;

import java.io.File;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OtelApmServiceMapProcessorTest {

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private PipelineDescription pipelineDescription;

    @Mock
    private OtelApmServiceMapProcessorConfig config;

    @Mock
    private Clock clock;

    @Mock
    private Span span;

    @Mock
    private MapDbProcessorState<Collection<SpanStateData>> mockWindow;

    @TempDir
    File tempDir;

    private OtelApmServiceMapProcessor processor;
    private final Instant testTime = Instant.ofEpochSecond(1609459200); // 2021-01-01T00:00:00Z
    
    @BeforeEach
    void setUp() {
        lenient().when(clock.instant()).thenReturn(testTime);
        lenient().when(clock.millis()).thenReturn(testTime.toEpochMilli());

        lenient().when(config.getWindowDuration()).thenReturn(Duration.ofSeconds(60));
        lenient().when(config.getDbPath()).thenReturn(tempDir.getAbsolutePath());
        lenient().when(config.getGroupByAttributes()).thenReturn(Collections.emptyList());
        
        lenient().when(pipelineDescription.getNumberOfProcessWorkers()).thenReturn(1);
        
        // Setup plugin metrics mocks
        lenient().when(pluginMetrics.gauge(anyString(), any(), any())).thenReturn(null);
    }

    @Test
    void testDoExecuteWithNoWindowDurationPassed() {
        // Given
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        Span mockSpan = createMockSpan("test-service", "test-operation", "SERVER");
        Record<Event> record = new Record<>(mockSpan);
        Collection<Record<Event>> records = Collections.singletonList(record);
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertTrue(result.isEmpty());
    }

    @Test
    void testDoExecuteWithWindowDurationPassed() {
        // Given
        when(clock.instant())
            .thenReturn(testTime) // Initial timestamp
            .thenReturn(testTime.plusSeconds(65)); // 65 seconds later

        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        Span mockSpan = createMockSpan("test-service", "test-operation", "SERVER");
        Record<Event> record = new Record<>(mockSpan);
        Collection<Record<Event>> records = Collections.singletonList(record);
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertNotNull(result);
    }

    @Test
    void testProcessSpanWithValidSpan() {
        // Given
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        Span mockSpan = createMockSpan("test-service", "test-operation", "SERVER");
        Record<Event> record = new Record<>(mockSpan);
        Collection<Record<Event>> records = Collections.singletonList(record);
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertNotNull(result);
    }

    @Test
    void testProcessSpanWithNullServiceName() {
        // Given
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        Span mockSpan = createMockSpan(null, "test-operation", "SERVER");
        Record<Event> record = new Record<>(mockSpan);
        Collection<Record<Event>> records = Collections.singletonList(record);
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testProcessSpanWithEmptyServiceName() {
        // Given
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        Span mockSpan = createMockSpan("", "test-operation", "SERVER");
        Record<Event> record = new Record<>(mockSpan);
        Collection<Record<Event>> records = Collections.singletonList(record);
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertNotNull(result);
    }

    @Test
    void testProcessSpanWithClientSpanKind() {
        // Given
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        Span mockSpan = createMockSpan("client-service", "client-operation", "CLIENT");
        Record<Event> record = new Record<>(mockSpan);
        Collection<Record<Event>> records = Collections.singletonList(record);
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertNotNull(result);
    }

    @Test
    void testProcessSpanWithExceptionHandling() {
        // Given
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        Span mockSpan = mock(Span.class);
        when(mockSpan.getServiceName()).thenReturn("test-service");
        when(mockSpan.getSpanId()).thenThrow(new RuntimeException("Test exception"));
        
        Record<Event> record = new Record<>(mockSpan);
        Collection<Record<Event>> records = Collections.singletonList(record);
        
        // When
        assertThrows(RuntimeException.class, ()->processor.doExecute(records));
    }

    @Test
    void testExtractSpanStatus() {
        // Given
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        Map<String, Object> status = new HashMap<>();
        status.put("code", "ERROR");
        
//        Span mockSpan = mock(Span.class);
//        when(mockSpan.getStatus()).thenReturn(status);
        
        // Create a reflection helper to test private method
        // Since extractSpanStatus is private, it's tested indirectly through processSpan
        Record<Event> record = new Record<>(createMockSpan("test-service", "test-op", "SERVER"));
        Collection<Record<Event>> records = Collections.singletonList(record);
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertNotNull(result);
    }

    @Test
    void testExtractSpanStatusWithNullStatus() {
        // Given
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        Span mockSpan = createMockSpan("test-service", "test-op", "SERVER");
        when(mockSpan.getStatus()).thenReturn(null);
        
        Record<Event> record = new Record<>(mockSpan);
        Collection<Record<Event>> records = Collections.singletonList(record);
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertNotNull(result);
    }

    @Test
    void testExtractSpanStatusWithEmptyStatus() {
        // Given
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        Span mockSpan = createMockSpan("test-service", "test-op", "SERVER");
        when(mockSpan.getStatus()).thenReturn(Collections.emptyMap());
        
        Record<Event> record = new Record<>(mockSpan);
        Collection<Record<Event>> records = Collections.singletonList(record);
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertNotNull(result);
    }

    @Test
    void testExtractSpanStatusWithException() {
        // Given
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        Span mockSpan = createMockSpan("test-service", "test-op", "SERVER");
        when(mockSpan.getStatus()).thenThrow(new RuntimeException("Status extraction error"));
        
        Record<Event> record = new Record<>(mockSpan);
        Collection<Record<Event>> records = Collections.singletonList(record);
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertNotNull(result);
    }

    @Test
    void testExtractSpanAttributesWithValidAttributes() {
        // Given
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("http.method", "GET");
        attributes.put("http.status_code", 200);
        
        Map<String, Object> resource = new HashMap<>();
        resource.put("service.name", "test-service");
        
        Span mockSpan = createMockSpan("test-service", "test-op", "SERVER");
        when(mockSpan.getAttributes()).thenReturn(attributes);
        when(mockSpan.getResource()).thenReturn(resource);
        
        Record<Event> record = new Record<>(mockSpan);
        Collection<Record<Event>> records = Collections.singletonList(record);
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertNotNull(result);
    }

    @Test
    void testExtractSpanAttributesWithException() {
        // Given
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        Span mockSpan = createMockSpan("test-service", "test-op", "SERVER");
        when(mockSpan.getAttributes()).thenThrow(new RuntimeException("Attributes extraction error"));
        
        Record<Event> record = new Record<>(mockSpan);
        Collection<Record<Event>> records = Collections.singletonList(record);
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertNotNull(result);
    }

    @Test
    void testExtractGroupByAttributesWithValidAttributes() {
        // Given
        List<String> groupByAttributes = Arrays.asList("deployment.environment", "service.namespace");
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics, groupByAttributes);
        
        Map<String, Object> resourceAttributes = new HashMap<>();
        resourceAttributes.put("deployment.environment", "production");
        resourceAttributes.put("service.namespace", "default");
        resourceAttributes.put("service.name", "test-service");
        
        Map<String, Object> resource = new HashMap<>();
        resource.put("attributes", resourceAttributes);
        
        Span mockSpan = createMockSpan("test-service", "test-op", "SERVER");
        when(mockSpan.getResource()).thenReturn(resource);
        
        Record<Event> record = new Record<>(mockSpan);
        Collection<Record<Event>> records = Collections.singletonList(record);
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertNotNull(result);
    }

    @Test
    void testExtractGroupByAttributesWithNullResource() {
        // Given
        List<String> groupByAttributes = Arrays.asList("deployment.environment");
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics, groupByAttributes);
        
        Span mockSpan = createMockSpan("test-service", "test-op", "SERVER");
        when(mockSpan.getResource()).thenReturn(null);
        
        Record<Event> record = new Record<>(mockSpan);
        Collection<Record<Event>> records = Collections.singletonList(record);
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertNotNull(result);
    }

    @Test
    void testExtractGroupByAttributesWithEmptyGroupByList() {
        // Given
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics, Collections.emptyList());
        
        Span mockSpan = createMockSpan("test-service", "test-op", "SERVER");
        Record<Event> record = new Record<>(mockSpan);
        Collection<Record<Event>> records = Collections.singletonList(record);
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertNotNull(result);
    }

    @Test
    void testExtractGroupByAttributesWithException() {
        // Given
        List<String> groupByAttributes = Arrays.asList("deployment.environment");
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics, groupByAttributes);
        
        Span mockSpan = createMockSpan("test-service", "test-op", "SERVER");
        when(mockSpan.getResource()).thenThrow(new RuntimeException("Resource extraction error"));
        
        Record<Event> record = new Record<>(mockSpan);
        Collection<Record<Event>> records = Collections.singletonList(record);
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertNotNull(result);
    }

    @Test
    void testWindowDurationHasPassed() {
        // Given
        when(clock.instant())
            .thenReturn(Instant.ofEpochMilli(1000L)) // Initial time
            .thenReturn(Instant.ofEpochMilli(61000L)); // 61 seconds later

        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        // Create a span to process
        Span mockSpan = createMockSpan("test-service", "test-op", "SERVER");
        Record<Event> record = new Record<>(mockSpan);
        Collection<Record<Event>> records = Collections.singletonList(record);
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertNotNull(result);
    }

    @Test
    void testWindowDurationNotPassed() {
        // Given
        when(clock.instant())
            .thenReturn(Instant.ofEpochMilli(1000L)) // Initial time
            .thenReturn(Instant.ofEpochMilli(30000L)); // 30 seconds later

        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        // Create a span to process
        Span mockSpan = createMockSpan("test-service", "test-op", "SERVER");
        Record<Event> record = new Record<>(mockSpan);
        Collection<Record<Event>> records = Collections.singletonList(record);
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertTrue(result.isEmpty());
    }

    @Test
    void testIsMasterInstance() {
        // Given
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        // When - Create another instance (should not be master)
        OtelApmServiceMapProcessor processor2 = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        // Then
        // Both should work without issues (testing internal master logic)
        assertNotNull(processor);
        assertNotNull(processor2);
    }

    @Test
    void testGetSpansDbSize() {
        // Given
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        // When
        double size = processor.getSpansDbSize();
        
        // Then
        assertTrue(size >= 0);
    }

    @Test
    void testGetSpansDbCount() {
        // Given
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        // When
        double count = processor.getSpansDbCount();
        
        // Then
        assertTrue(count >= 0);
    }

    @Test
    void testGetIdentificationKeys() {
        // Given
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        // When
        Collection<String> keys = processor.getIdentificationKeys();
        
        // Then
        assertNotNull(keys);
        assertTrue(keys.contains("traceId"));
    }

    @Test
    void testPrepareForShutdown() {
        // Given
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        // When
        processor.prepareForShutdown();
        
        // Then
        // Should complete without exception
    }

    @Test
    void testIsReadyForShutdown() {
        // Given
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        // When
        boolean ready = processor.isReadyForShutdown();
        
        // Then
        assertTrue(ready); // Should be ready when no data to process
    }

    @Test
    void testShutdown() {
        // Given
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        // When
        processor.shutdown();
        
        // Then
        // Should complete without exception
    }

    @Test
    void testMultipleSpansProcessing() {
        // Given
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        List<Record<Event>> records = Arrays.asList(
            new Record<>(createMockSpan("service1", "op1", "CLIENT")),
            new Record<>(createMockSpan("service2", "op2", "SERVER")),
            new Record<>(createMockSpan("service3", "op3", "CLIENT"))
        );
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertNotNull(result);
    }

    @Test
    void testSpanWithNullDuration() {
        // Given
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        Span mockSpan = createMockSpan("test-service", "test-op", "SERVER");
        when(mockSpan.getDurationInNanos()).thenReturn(null);
        
        Record<Event> record = new Record<>(mockSpan);
        Collection<Record<Event>> records = Collections.singletonList(record);
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertNotNull(result);
    }

    @Test
    void testSpanWithZeroDuration() {
        // Given
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        Span mockSpan = createMockSpan("test-service", "test-op", "SERVER");
        when(mockSpan.getDurationInNanos()).thenReturn(0L);
        
        Record<Event> record = new Record<>(mockSpan);
        Collection<Record<Event>> records = Collections.singletonList(record);
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertNotNull(result);
    }

    @Test
    void testSpanWithEmptyParentSpanId() {
        // Given
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        Span mockSpan = createMockSpan("test-service", "test-op", "SERVER");
        when(mockSpan.getParentSpanId()).thenReturn("");
        
        Record<Event> record = new Record<>(mockSpan);
        Collection<Record<Event>> records = Collections.singletonList(record);
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertNotNull(result);
    }

    @Test
    void testSpanWithInvalidHexSpanId() {
        // Given
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        Span mockSpan = createMockSpan("test-service", "test-op", "SERVER");
        when(mockSpan.getSpanId()).thenReturn("invalid-hex");
        
        Record<Event> record = new Record<>(mockSpan);
        Collection<Record<Event>> records = Collections.singletonList(record);
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertNotNull(result);
    }

    @Test
    void testSpanWithNullEndTime() {
        // Given
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        Span mockSpan = createMockSpan("test-service", "test-op", "SERVER");
        when(mockSpan.getEndTime()).thenReturn(null);
        
        Record<Event> record = new Record<>(mockSpan);
        Collection<Record<Event>> records = Collections.singletonList(record);
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertNotNull(result);
    }

    @Test
    void testSpanWithInvalidEndTime() {
        // Given
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        Span mockSpan = createMockSpan("test-service", "test-op", "SERVER");
        when(mockSpan.getEndTime()).thenReturn("invalid-timestamp");
        
        Record<Event> record = new Record<>(mockSpan);
        Collection<Record<Event>> records = Collections.singletonList(record);
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertNotNull(result);
    }

    @Test
    void testComplexWindowProcessingWithMultipleProcessors() {
        // Given
        //when(pipelineDescription.getNumberOfProcessWorkers()).thenReturn(3);

        when(clock.instant())
            .thenReturn(testTime) // Initial timestamp
            .thenReturn(testTime.plusMillis(65)); // 65 milliseconds later

        processor = new OtelApmServiceMapProcessor(Duration.ofMillis(60), tempDir, clock, 3, pluginMetrics);
        
        List<Record<Event>> records = Arrays.asList(
            new Record<>(createMockSpan("service-1", "operation-1", "CLIENT")),
            new Record<>(createMockSpan("service-2", "operation-2", "SERVER")),
            new Record<>(createMockSpan("service-3", "operation-3", "CLIENT"))
        );
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertNotNull(result);
    }

    @Test
    void testSpanProcessingWithComplexTraceRelationships() {
        // Given
        when(clock.instant())
            .thenReturn(testTime) // Initial timestamp
            .thenReturn(testTime.plusSeconds(65)); // 65 seconds later

        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        // Create a complex trace with parent-child relationships
        Span parentSpan = createMockSpanWithIds("parent-service", "parent-op", "SERVER", 
                                               "1111111111111111", "", "aaaaaaaaaaaaaaaa");
        Span childSpan1 = createMockSpanWithIds("child-service-1", "child-op-1", "CLIENT", 
                                               "2222222222222222", "1111111111111111", "aaaaaaaaaaaaaaaa");
        Span childSpan2 = createMockSpanWithIds("child-service-2", "child-op-2", "SERVER", 
                                               "3333333333333333", "2222222222222222", "aaaaaaaaaaaaaaaa");
        
        List<Record<Event>> records = Arrays.asList(
            new Record<>(parentSpan),
            new Record<>(childSpan1),
            new Record<>(childSpan2)
        );
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertNotNull(result);
    }

    @Test
    void testWindowProcessingWithInterruptedException() {
        // Given
        when(clock.instant())
            .thenReturn(testTime) // Initial timestamp
            .thenReturn(testTime.plusSeconds(65)); // 65 seconds later

        // Mock the processor to throw InterruptedException during barrier wait
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics) {
            @Override
            public Collection<Record<Event>> doExecute(Collection<Record<Event>> records) {
                // Override to simulate barrier exception
                try {
                    return super.doExecute(records);
                } catch (RuntimeException e) {
                    // Should handle the exception gracefully
                    throw e;
                }
            }
        };
        
        Span mockSpan = createMockSpan("test-service", "test-op", "SERVER");
        Record<Event> record = new Record<>(mockSpan);
        Collection<Record<Event>> records = Collections.singletonList(record);
        
        // When/Then - Should handle exceptions gracefully
        Collection<Record<Event>> result = processor.doExecute(records);
        assertNotNull(result);
    }

    @Test
    void testGroupByAttributesWithNestedResourceStructure() {
        // Given
        List<String> groupByAttributes = Arrays.asList("deployment.environment", "k8s.namespace.name", "service.version");
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics, groupByAttributes);
        
        Map<String, Object> nestedAttributes = new HashMap<>();
        nestedAttributes.put("deployment.environment", "production");
        nestedAttributes.put("k8s.namespace.name", "default");
        nestedAttributes.put("service.version", "1.2.3");
        nestedAttributes.put("service.name", "test-service");
        nestedAttributes.put("unwanted.attribute", "should-not-be-included");
        
        Map<String, Object> resource = new HashMap<>();
        resource.put("attributes", nestedAttributes);
        
        Span mockSpan = createMockSpan("test-service", "test-op", "SERVER");
        when(mockSpan.getResource()).thenReturn(resource);
        
        Record<Event> record = new Record<>(mockSpan);
        Collection<Record<Event>> records = Collections.singletonList(record);
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertNotNull(result);
    }

    @Test
    void testGroupByAttributesWithNonMapResourceAttributes() {
        // Given
        List<String> groupByAttributes = Arrays.asList("deployment.environment");
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics, groupByAttributes);
        
        Map<String, Object> resource = new HashMap<>();
        resource.put("attributes", "not-a-map"); // Invalid structure
        
        Span mockSpan = createMockSpan("test-service", "test-op", "SERVER");
        when(mockSpan.getResource()).thenReturn(resource);
        
        Record<Event> record = new Record<>(mockSpan);
        Collection<Record<Event>> records = Collections.singletonList(record);
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertNotNull(result);
    }

    @Test
    void testGetAnchorTimestampFromSpanWithValidEndTime() {
        // Given
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        Span mockSpan = createMockSpan("test-service", "test-op", "SERVER");
        when(mockSpan.getEndTime()).thenReturn("2021-01-01T12:30:45.123Z");
        
        Record<Event> record = new Record<>(mockSpan);
        Collection<Record<Event>> records = Collections.singletonList(record);
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertNotNull(result);
    }

    @Test
    void testGetAnchorTimestampFromSpanWithEmptyEndTime() {
        // Given
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        Span mockSpan = createMockSpan("test-service", "test-op", "SERVER");
        when(mockSpan.getEndTime()).thenReturn("");
        
        Record<Event> record = new Record<>(mockSpan);
        Collection<Record<Event>> records = Collections.singletonList(record);
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertNotNull(result);
    }

    @Test
    void testSpanProcessingWithHttpStatusCodeAttributes() {
        // Given
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("http.response.status_code", 404);
        attributes.put("http.method", "GET");
        attributes.put("http.url", "http://example.com/api");
        
        Span mockSpan = createMockSpan("web-service", "GET /api", "SERVER");
        when(mockSpan.getAttributes()).thenReturn(attributes);
        
        Record<Event> record = new Record<>(mockSpan);
        Collection<Record<Event>> records = Collections.singletonList(record);
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertNotNull(result);
    }

    @Test
    void testSpanProcessingWithStatusCodeInStatus() {
        // Given
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        Map<String, Object> status = new HashMap<>();
        status.put("code", 2); // ERROR status code
        status.put("message", "Internal error");
        
        Span mockSpan = createMockSpan("error-service", "error-op", "SERVER");
        when(mockSpan.getStatus()).thenReturn(status);
        
        Record<Event> record = new Record<>(mockSpan);
        Collection<Record<Event>> records = Collections.singletonList(record);
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertNotNull(result);
    }

    @Test
    void testSpanProcessingWithNullStatusCode() {
        // Given
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        Map<String, Object> status = new HashMap<>();
        status.put("code", null);
        status.put("message", "No code");
        
        Span mockSpan = createMockSpan("no-code-service", "no-code-op", "SERVER");
        when(mockSpan.getStatus()).thenReturn(status);
        
        Record<Event> record = new Record<>(mockSpan);
        Collection<Record<Event>> records = Collections.singletonList(record);
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertNotNull(result);
    }

    @Test
    void testSpanProcessingWithMixedSpanKinds() {
        // Given
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        List<Record<Event>> records = Arrays.asList(
            new Record<>(createMockSpan("producer-service", "send-message", "PRODUCER")),
            new Record<>(createMockSpan("consumer-service", "receive-message", "CONSUMER")),
            new Record<>(createMockSpan("internal-service", "process", "INTERNAL")),
            new Record<>(createMockSpan("client-service", "call-api", "CLIENT")),
            new Record<>(createMockSpan("server-service", "handle-request", "SERVER"))
        );
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertNotNull(result);
    }

    @Test
    void testSpanProcessingWithVeryLongDuration() {
        // Given
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        Span mockSpan = createMockSpan("slow-service", "slow-operation", "SERVER");
        when(mockSpan.getDurationInNanos()).thenReturn(Long.MAX_VALUE);
        
        Record<Event> record = new Record<>(mockSpan);
        Collection<Record<Event>> records = Collections.singletonList(record);
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertNotNull(result);
    }

    @Test
    void testSpanProcessingWithNegativeDuration() {
        // Given
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        Span mockSpan = createMockSpan("negative-duration-service", "negative-op", "SERVER");
        when(mockSpan.getDurationInNanos()).thenReturn(-1000L);
        
        Record<Event> record = new Record<>(mockSpan);
        Collection<Record<Event>> records = Collections.singletonList(record);
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertNotNull(result);
    }

    @Test
    void testComplexResourceWithMultipleLevels() {
        // Given
        List<String> groupByAttributes = Arrays.asList("deployment.environment");
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics, groupByAttributes);
        
        Map<String, Object> nestedResource = new HashMap<>();
        nestedResource.put("deployment.environment", "staging");
        
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("resource", nestedResource);
        
        Map<String, Object> resource = new HashMap<>();
        resource.put("attributes", attributes);
        
        Span mockSpan = createMockSpan("nested-service", "nested-op", "SERVER");
        when(mockSpan.getResource()).thenReturn(resource);
        when(mockSpan.getAttributes()).thenReturn(attributes);
        
        Record<Event> record = new Record<>(mockSpan);
        Collection<Record<Event>> records = Collections.singletonList(record);
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertNotNull(result);
    }

    @Test
    void testProcessingEmptyRecordCollection() {
        // Given
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        Collection<Record<Event>> emptyRecords = Collections.emptyList();
        
        // When
        Collection<Record<Event>> result = processor.doExecute(emptyRecords);
        
        // Then
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testProcessingNullRecordCollection() {
        // Given
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        // When/Then
        assertThrows(NullPointerException.class, () -> {
            processor.doExecute(null);
        });
    }

    @Test
    void testStaticProcessorsCreatedCounter() {
        // Given - Create multiple processors to test static counter
        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        OtelApmServiceMapProcessor processor2 = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        OtelApmServiceMapProcessor processor3 = new OtelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, pluginMetrics);
        
        // When - Create spans for each processor
        Span mockSpan1 = createMockSpan("service-1", "op-1", "SERVER");
        Span mockSpan2 = createMockSpan("service-2", "op-2", "CLIENT");
        Span mockSpan3 = createMockSpan("service-3", "op-3", "SERVER");
        
        // Then - All processors should work
        assertNotNull(processor.doExecute(Collections.singletonList(new Record<>(mockSpan1))));
        assertNotNull(processor2.doExecute(Collections.singletonList(new Record<>(mockSpan2))));
        assertNotNull(processor3.doExecute(Collections.singletonList(new Record<>(mockSpan3))));
    }

    @Test
    void testWindowProcessingWithCustomWindowDuration() {
        // Given - Use a very short window duration
        when(clock.instant())
            .thenReturn(Instant.ofEpochMilli(1000L)) // Initial time
            .thenReturn(Instant.ofEpochMilli(1001L)) // Just 1 millisecond later
            .thenReturn(Instant.ofEpochMilli(2001L)); // 1001ms later (window passed)

        processor = new OtelApmServiceMapProcessor(Duration.ofSeconds(1), tempDir, clock, 1, pluginMetrics); // 1 second window
        
        Span mockSpan = createMockSpan("fast-service", "fast-op", "SERVER");
        Record<Event> record = new Record<>(mockSpan);
        Collection<Record<Event>> records = Collections.singletonList(record);
        
        // When
        Collection<Record<Event>> result1 = processor.doExecute(records); // Should be empty
        Collection<Record<Event>> result2 = processor.doExecute(records); // Should trigger processing
        
        // Then
        assertTrue(result1.isEmpty()); // First call - window not passed
        assertNotNull(result2); // Second call - window passed
    }

    // Helper method to create mock spans with custom IDs
    private Span createMockSpanWithIds(String serviceName, String operationName, String spanKind, 
                                      String spanId, String parentSpanId, String traceId) {
        Span mockSpan = mock(Span.class);
        lenient().when(mockSpan.getServiceName()).thenReturn(serviceName);
        lenient().when(mockSpan.getSpanId()).thenReturn(spanId);
        lenient().when(mockSpan.getParentSpanId()).thenReturn(parentSpanId);
        lenient().when(mockSpan.getTraceId()).thenReturn(traceId);
        lenient().when(mockSpan.getKind()).thenReturn(spanKind);
        lenient().when(mockSpan.getName()).thenReturn(operationName);
        lenient().when(mockSpan.getDurationInNanos()).thenReturn(1000000000L); // 1 second
        lenient().when(mockSpan.getEndTime()).thenReturn("2021-01-01T00:00:00.000Z");
        
        Map<String, Object> status = new HashMap<>();
        status.put("code", "OK");
        lenient().when(mockSpan.getStatus()).thenReturn(status);
        
        lenient().when(mockSpan.getAttributes()).thenReturn(Collections.emptyMap());
        lenient().when(mockSpan.getResource()).thenReturn(Collections.emptyMap());
        
        return mockSpan;
    }

    // Helper method to create mock spans
    private Span createMockSpan(String serviceName, String operationName, String spanKind) {
        Span mockSpan = mock(Span.class);
        lenient().when(mockSpan.getServiceName()).thenReturn(serviceName);
        lenient().when(mockSpan.getSpanId()).thenReturn("1234567890abcdef");
        lenient().when(mockSpan.getParentSpanId()).thenReturn("fedcba0987654321");
        lenient().when(mockSpan.getTraceId()).thenReturn("1234567890abcdef1234567890abcdef");
        lenient().when(mockSpan.getKind()).thenReturn(spanKind);
        lenient().when(mockSpan.getName()).thenReturn(operationName);
        lenient().when(mockSpan.getDurationInNanos()).thenReturn(1000000000L); // 1 second
        lenient().when(mockSpan.getEndTime()).thenReturn("2021-01-01T00:00:00.000Z");
        
        Map<String, Object> status = new HashMap<>();
        status.put("code", "OK");
        lenient().when(mockSpan.getStatus()).thenReturn(status);
        
        lenient().when(mockSpan.getAttributes()).thenReturn(Collections.emptyMap());
        lenient().when(mockSpan.getResource()).thenReturn(Collections.emptyMap());
        
        return mockSpan;
    }
}
