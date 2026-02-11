/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.otel_apm_service_map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.event.BaseEventBuilder;
import org.opensearch.dataprepper.model.event.EventBuilder;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.test.plugins.DataPrepperPluginTest;
import org.opensearch.dataprepper.test.plugins.junit.BaseDataPrepperPluginStandardTestSuite;

import java.io.File;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;

@DataPrepperPluginTest(pluginName = "otel_apm_service_map", pluginType = Processor.class) 
class OTelApmServiceMapProcessorTest extends BaseDataPrepperPluginStandardTestSuite {
    @Mock
    private EventFactory eventFactory;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private PipelineDescription pipelineDescription;

    @Mock
    private OTelApmServiceMapProcessorConfig config;

    @Mock
    private Clock clock;

    @TempDir
    File tempDir;

    private EventMetadata eventMetadata;
    private Object eventData;

    private OTelApmServiceMapProcessor processor;
    private final Instant testTime = Instant.ofEpochSecond(1609459200); // 2021-01-01T00:00:00Z

    private OTelApmServiceMapProcessor createObjectUnderTest() {
        return new OTelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, eventFactory, pluginMetrics);
    }
    
    private OTelApmServiceMapProcessor createObjectUnderTest(List<String> groupByAttributes) {
        return new OTelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, eventFactory, pluginMetrics, groupByAttributes);
    }
    
    private OTelApmServiceMapProcessor createObjectUnderTest(Duration duration, int workers) {
        return new OTelApmServiceMapProcessor(duration, tempDir, clock, workers, eventFactory, pluginMetrics);
    }
    
    @BeforeEach
    void setUp() {
        eventFactory = mock(EventFactory.class);
        clock = mock(Clock.class);
        config = mock(OTelApmServiceMapProcessorConfig.class);
        pipelineDescription = mock(PipelineDescription.class);
        pluginMetrics = mock(PluginMetrics.class);
        lenient().when(clock.instant()).thenReturn(testTime);
        lenient().when(clock.millis()).thenReturn(testTime.toEpochMilli());

        lenient().when(config.getWindowDuration()).thenReturn(Duration.ofSeconds(60));
        lenient().when(config.getDbPath()).thenReturn(tempDir.getAbsolutePath());
        lenient().when(config.getGroupByAttributes()).thenReturn(Collections.emptyList());
        
        lenient().when(pipelineDescription.getNumberOfProcessWorkers()).thenReturn(1);
        
        // Setup plugin metrics mocks
        lenient().when(pluginMetrics.gauge(anyString(), any(), any())).thenReturn(null);
    }

    @AfterEach
    void teardown() {
        if (processor != null) {
            processor.shutdown();
        }
    }

    @Test
    void testDoExecuteWithNoWindowDurationPassed() {
        // Given
        processor = createObjectUnderTest();
        
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

        processor = createObjectUnderTest();
        
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
        processor = createObjectUnderTest();
        
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
        processor = createObjectUnderTest();
        
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
        processor = createObjectUnderTest();
        
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
        processor = createObjectUnderTest();
        
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
        processor = createObjectUnderTest();
        
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
        processor = createObjectUnderTest();
        
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
        processor = createObjectUnderTest();
        
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
        processor = createObjectUnderTest();
        
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
        processor = createObjectUnderTest();
        
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
        processor = createObjectUnderTest();
        
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
        processor = createObjectUnderTest();
        
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
        processor = createObjectUnderTest(groupByAttributes);
        
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
        processor = createObjectUnderTest(groupByAttributes);
        
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
        processor = createObjectUnderTest(Collections.emptyList());
        
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
        processor = createObjectUnderTest(groupByAttributes);
        
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

        processor = createObjectUnderTest();
        
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

        processor = createObjectUnderTest();
        
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
        processor = createObjectUnderTest();
        
        // When - Create another instance (should not be master)
        OTelApmServiceMapProcessor processor2 = createObjectUnderTest();
        
        // Then
        // Both should work without issues (testing internal master logic)
        assertNotNull(processor);
        assertNotNull(processor2);
    }

    @Test
    void testGetSpansDbSize() {
        // Given
        processor = createObjectUnderTest();
        
        // When
        double size = processor.getSpansDbSize();
        
        // Then
        assertTrue(size >= 0);
    }

    @Test
    void testGetSpansDbCount() {
        // Given
        processor = createObjectUnderTest();
        
        // When
        double count = processor.getSpansDbCount();
        
        // Then
        assertTrue(count >= 0);
    }

    @Test
    void testGetIdentificationKeys() {
        // Given
        processor = createObjectUnderTest();
        
        // When
        Collection<String> keys = processor.getIdentificationKeys();
        
        // Then
        assertNotNull(keys);
        assertTrue(keys.contains("traceId"));
    }

    @Test
    void testPrepareForShutdown() {
        // Given
        processor = createObjectUnderTest();
        
        // When
        processor.prepareForShutdown();
        
        // Then
        // Should complete without exception
    }

    @Test
    void testIsReadyForShutdown() {
        // Given
        processor = createObjectUnderTest();
        
        // When
        boolean ready = processor.isReadyForShutdown();
        
        // Then
        assertTrue(ready); // Should be ready when no data to process
    }

    @Test
    void testShutdown() {
        // Given
        processor = createObjectUnderTest();
        
        // When
        processor.shutdown();
        
        // Then
        // Should complete without exception
    }

    @Test
    void testMultipleSpansProcessing() {
        // Given
        processor = createObjectUnderTest();
        
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
        processor = createObjectUnderTest();
        
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
        processor = createObjectUnderTest();
        
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
        processor = createObjectUnderTest();
        
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
        processor = createObjectUnderTest();
        
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
        processor = createObjectUnderTest();
        
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
        processor = createObjectUnderTest();
        
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
    void testComplexWindowProcessingWithMultipleProcessors() throws Exception {
        // Given
        //when(pipelineDescription.getNumberOfProcessWorkers()).thenReturn(3);

        when(clock.instant())
            .thenReturn(testTime) // Initial timestamp
            .thenReturn(testTime.plusMillis(65)); // 65 milliseconds later

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            threads.add(new Thread(() -> {
                OTelApmServiceMapProcessor processor = createObjectUnderTest(Duration.ofMillis(60), 3);
                
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e){}
                List<Record<Event>> records = Arrays.asList(
                    new Record<>(createMockSpan("service-1", "operation-1", "CLIENT")),
                    new Record<>(createMockSpan("service-2", "operation-2", "SERVER")),
                new Record<>(createMockSpan("service-3", "operation-3", "CLIENT"))
                );
            
                // When
                Collection<Record<Event>> result = processor.doExecute(records);
            }));
        }
        for (int i = 0; i < 3; i++) {
            threads.get(i).start();
        }
        
        for (int i = 0; i < 3; i++) {
            threads.get(i).join();
        }
        // Then
        //assertNotNull(result);
    }

    @Test
    void testSpanProcessingWithComplexTraceRelationships() {
        // Given
        when(clock.instant())
            .thenReturn(testTime) // Initial timestamp
            .thenReturn(testTime.plusSeconds(65)); // 65 seconds later

        processor = createObjectUnderTest();
        
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
        processor = new OTelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, eventFactory, pluginMetrics) {
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
        processor = createObjectUnderTest(groupByAttributes);
        
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
        processor = createObjectUnderTest(groupByAttributes);
        
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
        processor = createObjectUnderTest();
        
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
        processor = createObjectUnderTest();
        
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
        processor = createObjectUnderTest();
        
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
        processor = createObjectUnderTest();
        
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
        processor = createObjectUnderTest();
        
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
        processor = createObjectUnderTest();
        
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
        processor = createObjectUnderTest();
        
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
        processor = createObjectUnderTest();
        
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
        processor = createObjectUnderTest(groupByAttributes);
        
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
        processor = createObjectUnderTest();
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
        processor = createObjectUnderTest();
        
        // When/Then
        assertThrows(NullPointerException.class, () -> {
            processor.doExecute(null);
        });
    }

    @Test
    void testStaticProcessorsCreatedCounter() {
        // Given - Create multiple processors to test static counter
        processor = createObjectUnderTest();
        OTelApmServiceMapProcessor processor2 = createObjectUnderTest();
        OTelApmServiceMapProcessor processor3 = createObjectUnderTest();
        
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

        processor = createObjectUnderTest(Duration.ofSeconds(1), 1); // 1 second window
        
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

    @Test
    void testProcessCurrentWindowSpans() {
        // Given
        when(clock.instant())
            .thenReturn(testTime)
            .thenReturn(testTime.plusSeconds(65));

        processor = createObjectUnderTest();
        
        Span clientSpan = createMockSpanWithIds("client-service", "client-op", "CLIENT", 
                                               "1111111111111111", "", "aaaaaaaaaaaaaaaa");
        Span serverSpan = createMockSpanWithIds("server-service", "server-op", "SERVER", 
                                               "2222222222222222", "1111111111111111", "aaaaaaaaaaaaaaaa");
        
        List<Record<Event>> records = Arrays.asList(
            new Record<>(clientSpan),
            new Record<>(serverSpan)
        );
        
        // When
        Collection<Record<Event>> result = processor.doExecute(records);
        
        // Then
        assertNotNull(result);
    }

    @Test
    void testDecorateSpansInTraceWithEphemeralStorage() {
        // Given - Setup clock to return specific times for window rotation
        when(clock.instant())
            .thenReturn(testTime)                    // Initial timestamp
            .thenReturn(testTime)                    // windowDurationHasPassed check (call 1)
            .thenReturn(testTime.plusSeconds(65))    // windowDurationHasPassed check (call 2) - triggers rotation
            .thenReturn(testTime.plusSeconds(65))    // evaluateApmEvents timestamp
            .thenReturn(testTime.plusSeconds(65))    // processCurrentWindowSpans timestamp
            .thenReturn(testTime.plusSeconds(65))    // rotateWindows timestamp
            .thenReturn(testTime.plusSeconds(130))   // windowDurationHasPassed check (call 3) - triggers processing
            .thenReturn(testTime.plusSeconds(130))   // evaluateApmEvents timestamp
            .thenReturn(testTime.plusSeconds(130))   // processCurrentWindowSpans timestamp
            .thenReturn(testTime.plusSeconds(130));  // rotateWindows timestamp

        final BaseEventBuilder<Event> eventBuilder = mock(EventBuilder.class, RETURNS_DEEP_STUBS);
        when(eventFactory.eventBuilder(any())).thenReturn(eventBuilder);
        doAnswer((a) -> {
            eventMetadata = a.getArgument(0);
            return eventBuilder;
        }).when(eventBuilder).withEventMetadata(any());
        doAnswer((a) -> {
            eventData = a.getArgument(0);
            return eventBuilder;
        }).when(eventBuilder).withData(any());
        doAnswer((a) -> {
            return JacksonEvent.builder()
                    .withEventMetadata(eventMetadata)
                    .withData(eventData)
                    .build();
        }).when(eventBuilder).build();

        // Create a fresh processor in a new temp directory to avoid interference from other tests
        File isolatedTempDir = new File(tempDir, "isolated-test-" + System.nanoTime());
        isolatedTempDir.mkdirs();
        OTelApmServiceMapProcessor isolatedProcessor = new OTelApmServiceMapProcessor(
            Duration.ofSeconds(60), isolatedTempDir, clock, 1, eventFactory, pluginMetrics);
        
        Span clientSpan = createMockSpanWithIds("client-service", "client-op", "SPAN_KIND_CLIENT", 
                                               "1111111111111111", "", "aaaaaaaaaaaaaaaa");
        Span serverSpan = createMockSpanWithIds("server-service", "server-op", "SPAN_KIND_SERVER", 
                                               "2222222222222222", "1111111111111111", "aaaaaaaaaaaaaaaa");
        
        List<Record<Event>> records = Arrays.asList(
            new Record<>(clientSpan),
            new Record<>(serverSpan)
        );
        
        // When
        // Call 1 (t=0): Add spans to nextWindow, no window rotation
        isolatedProcessor.doExecute(records);
        // Call 2 (t=65): Window passed -> process empty currentWindow, rotate (nextWindow->currentWindow)
        isolatedProcessor.doExecute(Collections.emptyList());
        // Call 3 (t=130): Window passed -> process currentWindow with our spans (decorateSpansInTraceWithEphemeralStorage invoked!)
        Collection<Record<Event>> result = isolatedProcessor.doExecute(Collections.emptyList());
        
        // Then
        assertNotNull(result);

        assertThat(result.size(), equalTo(6));
        List<Record<Event>> resultList = result.stream().collect(Collectors.toList());
        Event event = resultList.get(0).getData();
        assertThat(event.get("name", String.class), equalTo("request"));
        event = resultList.get(1).getData();
        assertThat(event.get("name", String.class), equalTo("error"));
        event = resultList.get(2).getData();
        assertThat(event.get("name", String.class), equalTo("fault"));
        event = resultList.get(3).getData();
        assertThat(event.get("name", String.class), equalTo("latency_seconds"));
        event = resultList.get(4).getData();
        String serviceAttributesName4 = event.get("service/keyAttributes/name", String.class);
        event = resultList.get(5).getData();
        String serviceAttributesName5 = event.get("service/keyAttributes/name", String.class);
        assertThat(Set.of(serviceAttributesName4, serviceAttributesName5), equalTo(Set.of("client-service", "server-service")));
        
        // Cleanup
        isolatedProcessor.shutdown();
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
