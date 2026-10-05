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

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.metric.JacksonSum;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.event.BaseEventBuilder;
import org.opensearch.dataprepper.model.event.EventBuilder;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.DependencyNamingPolicy;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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

    private Counter dependencyCallsOverflowedCounter;
    private Counter remoteOperationCallsOverflowedCounter;

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

    private OTelApmServiceMapProcessor createObjectUnderTest(MetricTimestampSource metricTimestampSource) {
        return new OTelApmServiceMapProcessor(Duration.ofSeconds(60), tempDir, clock, 1, eventFactory, pluginMetrics,
                Collections.emptyList(), metricTimestampSource);
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
        dependencyCallsOverflowedCounter = mock(Counter.class);
        remoteOperationCallsOverflowedCounter = mock(Counter.class);
        lenient().when(pluginMetrics.counter(OTelApmServiceMapProcessor.DEPENDENCY_CALLS_OVERFLOWED_METRIC))
                .thenReturn(dependencyCallsOverflowedCounter);
        lenient().when(pluginMetrics.counter(OTelApmServiceMapProcessor.REMOTE_OPERATION_CALLS_OVERFLOWED_METRIC))
                .thenReturn(remoteOperationCallsOverflowedCounter);
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
        final List<OTelApmServiceMapProcessor> processors = Collections.synchronizedList(new ArrayList<>());
        for (int i = 0; i < 3; i++) {
            threads.add(new Thread(() -> {
                OTelApmServiceMapProcessor processor = createObjectUnderTest(Duration.ofMillis(60), 3);
                processors.add(processor);
                
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
        // Reset the shared static window state so later tests do not wait on this 3-party barrier.
        processors.get(0).shutdown();
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
        assertThat(event.get("name", String.class), equalTo("latency"));
        event = resultList.get(4).getData();
        String sourceNodeName4 = event.get("sourceNode/keyAttributes/name", String.class);
        event = resultList.get(5).getData();
        String sourceNodeName5 = event.get("sourceNode/keyAttributes/name", String.class);
        assertThat(Set.of(sourceNodeName4, sourceNodeName5), equalTo(Set.of("client-service", "server-service")));
        
        // Cleanup
        isolatedProcessor.shutdown();
    }

    @Test
    void testArrivalTimeMode_usesClockInstant_notSpanEndTime() {
        // Given - arrival_time mode should use clock.instant() regardless of span endTime
        final Instant arrivalTime = Instant.parse("2021-01-01T00:05:00Z");
        final Instant arrivalTimePlusWindow = arrivalTime.plusSeconds(65);

        when(clock.instant())
            .thenReturn(arrivalTime)                            // 1. constructor: previousTimestamp
            .thenReturn(arrivalTime)                            // 2. doExecute call 1: windowDurationHasPassed
            .thenReturn(arrivalTimePlusWindow)                  // 3. doExecute call 2: windowDurationHasPassed
            .thenReturn(arrivalTimePlusWindow)                  // 4. processCurrentWindowSpans: currentTime
            .thenReturn(arrivalTimePlusWindow)                  // 5. rotateWindows: LOG.debug
            .thenReturn(arrivalTimePlusWindow)                  // 6. rotateWindows: previousTimestamp
            .thenReturn(arrivalTimePlusWindow.plusSeconds(65))  // 7. doExecute call 3: windowDurationHasPassed
            .thenReturn(arrivalTimePlusWindow.plusSeconds(65))  // 8. processCurrentWindowSpans: currentTime
            .thenReturn(arrivalTimePlusWindow.plusSeconds(65))  // 9. rotateWindows: LOG.debug
            .thenReturn(arrivalTimePlusWindow.plusSeconds(65)); // 10. rotateWindows: previousTimestamp

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

        File isolatedDir = new File(tempDir, "arrival-test-" + System.nanoTime());
        isolatedDir.mkdirs();
        OTelApmServiceMapProcessor arrivalProcessor = new OTelApmServiceMapProcessor(
            Duration.ofSeconds(60), isolatedDir, clock, 1, eventFactory, pluginMetrics,
            Collections.emptyList(), MetricTimestampSource.ARRIVAL_TIME);

        // Span with endTime at 12:30 — should be IGNORED in arrival_time mode
        Span span = createMockSpanWithIds("svc", "op", "SPAN_KIND_SERVER",
                "1111111111111111", "", "aaaaaaaaaaaaaaaa");
        when(span.getEndTime()).thenReturn("2021-01-01T12:30:45.123Z");

        arrivalProcessor.doExecute(Collections.singletonList(new Record<>(span)));
        arrivalProcessor.doExecute(Collections.emptyList());
        Collection<Record<Event>> result = arrivalProcessor.doExecute(Collections.emptyList());

        // Verify metrics exist and their timestamps use arrival time (NOT span endTime 12:30)
        assertThat(result.isEmpty(), equalTo(false));
        for (Record<Event> record : result) {
            Event event = record.getData();
            String time = event.get("time", String.class);
            if (time != null) {
                // arrival_time mode: timestamp should be from clock.instant(), NOT span's endTime (12:30)
                // Sum metrics truncate to seconds, Histogram to minutes — both should start with 00:07
                assertTrue(time.startsWith("2021-01-01T00:07:"),
                        "Expected arrival time (00:07:xx) but got: " + time);
            }
        }

        arrivalProcessor.shutdown();
    }

    @Test
    void testSpanEndTimeMode_usesSpanEndTime() {
        // Given - span_end_time mode should use span's endTime
        final Instant arrivalTime = Instant.parse("2021-01-01T00:05:00Z");
        final Instant arrivalTimePlusWindow = arrivalTime.plusSeconds(65);

        when(clock.instant())
            .thenReturn(arrivalTime)                            // 1. constructor: previousTimestamp
            .thenReturn(arrivalTime)                            // 2. doExecute call 1: windowDurationHasPassed
            .thenReturn(arrivalTimePlusWindow)                  // 3. doExecute call 2: windowDurationHasPassed
            .thenReturn(arrivalTimePlusWindow)                  // 4. processCurrentWindowSpans: currentTime
            .thenReturn(arrivalTimePlusWindow)                  // 5. rotateWindows: LOG.debug
            .thenReturn(arrivalTimePlusWindow)                  // 6. rotateWindows: previousTimestamp
            .thenReturn(arrivalTimePlusWindow.plusSeconds(65))  // 7. doExecute call 3: windowDurationHasPassed
            .thenReturn(arrivalTimePlusWindow.plusSeconds(65))  // 8. processCurrentWindowSpans: currentTime
            .thenReturn(arrivalTimePlusWindow.plusSeconds(65))  // 9. rotateWindows: LOG.debug
            .thenReturn(arrivalTimePlusWindow.plusSeconds(65)); // 10. rotateWindows: previousTimestamp

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

        File isolatedDir = new File(tempDir, "spanend-test-" + System.nanoTime());
        isolatedDir.mkdirs();
        OTelApmServiceMapProcessor spanEndProcessor = new OTelApmServiceMapProcessor(
            Duration.ofSeconds(60), isolatedDir, clock, 1, eventFactory, pluginMetrics,
            Collections.emptyList(), MetricTimestampSource.SPAN_END_TIME);

        // Span with endTime at 12:30:45 — should be used and truncated to 12:30:00
        Span span = createMockSpanWithIds("svc", "op", "SPAN_KIND_SERVER",
                "1111111111111111", "", "aaaaaaaaaaaaaaaa");
        when(span.getEndTime()).thenReturn("2021-01-01T12:30:45.123Z");

        spanEndProcessor.doExecute(Collections.singletonList(new Record<>(span)));
        spanEndProcessor.doExecute(Collections.emptyList());
        Collection<Record<Event>> result = spanEndProcessor.doExecute(Collections.emptyList());

        assertThat(result.isEmpty(), equalTo(false));
        for (Record<Event> record : result) {
            Event event = record.getData();
            String time = event.get("time", String.class);
            if (time != null) {
                // span_end_time mode: timestamp from span endTime (12:30:45), NOT arrival time (00:07)
                // Sum metrics truncate to seconds (12:30:45Z), Histogram to minutes (12:30:00Z)
                assertTrue(time.startsWith("2021-01-01T12:30:"),
                        "Expected span endTime (12:30:xx) but got: " + time);
            }
        }

        spanEndProcessor.shutdown();
    }

    @Test
    void doExecute_emitsServiceMapEventWithEnvironmentFromResource_whenScopeHasNullAttributes() {
        // Regression for issue #6786 (Bug 1): when the instrumentation scope has no
        // "attributes" key, OTelApmServiceMapProcessor.extractSpanAttributes used to
        // throw on putAll(null) and the catch returned an empty map, dropping the
        // resource and its deployment.environment.name.
        when(clock.instant())
                .thenReturn(testTime)
                .thenReturn(testTime)
                .thenReturn(testTime.plusSeconds(65))
                .thenReturn(testTime.plusSeconds(65))
                .thenReturn(testTime.plusSeconds(65))
                .thenReturn(testTime.plusSeconds(65))
                .thenReturn(testTime.plusSeconds(130))
                .thenReturn(testTime.plusSeconds(130))
                .thenReturn(testTime.plusSeconds(130))
                .thenReturn(testTime.plusSeconds(130));

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
        doAnswer((a) -> JacksonEvent.builder()
                .withEventMetadata(eventMetadata).withData(eventData).build())
                .when(eventBuilder).build();

        final File isolatedDir = new File(tempDir, "scope-null-attrs-" + System.nanoTime());
        isolatedDir.mkdirs();
        final OTelApmServiceMapProcessor isolatedProcessor = new OTelApmServiceMapProcessor(
                Duration.ofSeconds(60), isolatedDir, clock, 1, eventFactory, pluginMetrics);

        final Span leafServerSpan = createMockSpanWithIds("leaf-service", "GET /api/test",
                "SPAN_KIND_SERVER", "1111111111111111", "", "aaaaaaaaaaaaaaaa");
        final Map<String, Object> resourceAttrs = new HashMap<>();
        resourceAttrs.put("deployment.environment.name", "production");
        final Map<String, Object> resource = new HashMap<>();
        resource.put("attributes", resourceAttrs);
        when(leafServerSpan.getResource()).thenReturn(resource);
        // Scope present with NO "attributes" key — the trigger for Bug 1.
        final Map<String, Object> scope = new HashMap<>();
        scope.put("name", "my-tracer");
        when(leafServerSpan.getScope()).thenReturn(scope);

        try {
            isolatedProcessor.doExecute(Collections.singletonList(new Record<>(leafServerSpan)));
            isolatedProcessor.doExecute(Collections.emptyList());
            final Collection<Record<Event>> result = isolatedProcessor.doExecute(Collections.emptyList());

            assertNotNull(result);
            final Record<Event> serviceMapEvent = result.stream()
                    .filter(r -> r.getData().getMetadata() != null
                            && "SERVICE_MAP".equals(r.getData().getMetadata().getEventType()))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Expected at least one SERVICE_MAP event"));
            assertThat(serviceMapEvent.getData().get("sourceNode/keyAttributes/environment", String.class),
                    equalTo("production"));
        } finally {
            isolatedProcessor.shutdown();
        }
    }

    // ---- External-dependency synthesis (databases / messaging / external) ----

    @Test
    void synthesizesAwsSdkDependencyNode() {
        final OTelApmServiceMapProcessor proc = newFlushingProcessor("aws-dep-");
        try {
            final Span awsClient = createMockSpanWithIds("checkout", "DynamoDB.GetItem", "SPAN_KIND_CLIENT",
                    "1111111111111111", "", "aaaaaaaaaaaaaaaa");
            final Map<String, Object> attrs = new HashMap<>();
            attrs.put("rpc.system", "aws-api");
            attrs.put("rpc.service", "DynamoDb");
            attrs.put("rpc.method", "GetItem");
            when(awsClient.getAttributes()).thenReturn(attrs);

            final List<Event> serviceMap = flushServiceMapEvents(proc,
                    Collections.singletonList(new Record<>(awsClient)));

            final Event awsNode = serviceMap.stream()
                    .filter(e -> "external".equals(e.get("targetNode/type", String.class)))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Expected an AWS SDK dependency node"));
            assertThat(awsNode.get("targetNode/keyAttributes/name", String.class), equalTo("AWS::DynamoDB"));
            assertThat(awsNode.get("targetOperation/name", String.class), equalTo("GetItem"));
        } finally {
            proc.shutdown();
        }
    }

    @Test
    void synthesizesMessagingBrokerNodeForArnDestination() {
        final OTelApmServiceMapProcessor proc = newFlushingProcessor("arn-dep-");
        try {
            final Span producer = createMockSpanWithIds("checkout", "orders publish", "SPAN_KIND_PRODUCER",
                    "1111111111111111", "", "aaaaaaaaaaaaaaaa");
            final Map<String, Object> attrs = new HashMap<>();
            attrs.put("messaging.system", "aws_sns");
            attrs.put("messaging.destination.name", "arn:aws:sns:us-east-1:123456789012:orders");
            when(producer.getAttributes()).thenReturn(attrs);

            final List<Event> serviceMap = flushServiceMapEvents(proc,
                    Collections.singletonList(new Record<>(producer)));

            final Event brokerNode = serviceMap.stream()
                    .filter(e -> "messaging".equals(e.get("targetNode/type", String.class)))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Expected an SNS broker node"));
            assertThat(brokerNode.get("targetNode/keyAttributes/name", String.class),
                    equalTo("aws_sns:arn:aws:sns:us-east-1:123456789012:orders"));
        } finally {
            proc.shutdown();
        }
    }

    @Test
    void dependencyClientWithoutParentServer_emitsRequestMetricOnceAcrossAllWindows() {
        final Instant[] now = {testTime};
        when(clock.instant()).thenAnswer(a -> now[0]);
        final OTelApmServiceMapProcessor proc = newProcessorCapturingEvents("once-");
        try {
            final Span dbClient = createMockSpanWithIds("product-catalog", "query", "SPAN_KIND_CLIENT",
                    "1111111111111111", "", "aaaaaaaaaaaaaaaa");
            final Map<String, Object> attrs = new HashMap<>();
            attrs.put("db.system.name", "postgresql");
            when(dbClient.getAttributes()).thenReturn(attrs);
            final Span producer = createMockSpanWithIds("product-catalog", "orders publish", "SPAN_KIND_PRODUCER",
                    "2222222222222222", "", "aaaaaaaaaaaaaaaa");
            final Map<String, Object> producerAttrs = new HashMap<>();
            producerAttrs.put("messaging.system", "kafka");
            producerAttrs.put("messaging.destination.name", "orders");
            when(producer.getAttributes()).thenReturn(producerAttrs);

            // A later span of the same trace lands one window behind, so the trace is processed in two
            // consecutive cycles and the dependency spans are visible (as lookup spans) in both.
            final Span lateSpan = createMockSpanWithIds("product-catalog", "late", "SPAN_KIND_SERVER",
                    "3333333333333333", "", "aaaaaaaaaaaaaaaa");

            // Drive the spans through next -> current -> previous and out, flushing every window.
            final List<Record<Event>> emitted = new ArrayList<>(
                    proc.doExecute(Arrays.asList(new Record<>(dbClient), new Record<>(producer))));
            for (int window = 1; window <= 4; window++) {
                now[0] = testTime.plusSeconds(65L * window);
                emitted.addAll(proc.doExecute(window == 1
                        ? Collections.singletonList(new Record<>(lateSpan))
                        : Collections.emptyList()));
            }

            assertThat(sumOfRequestMetrics(emitted, "postgresql"), equalTo(1.0));
            assertThat(sumOfRequestMetrics(emitted, "kafka:orders"), equalTo(1.0));
        } finally {
            proc.shutdown();
        }
    }

    private static double sumOfRequestMetrics(final Collection<Record<Event>> records, final String remoteService) {
        return records.stream()
                .map(Record::getData)
                .filter(e -> e instanceof JacksonSum)
                .map(e -> (JacksonSum) e)
                .filter(m -> "request".equals(m.getName())
                        && remoteService.equals(m.getAttributes().get("remoteService")))
                .mapToDouble(JacksonSum::getValue)
                .sum();
    }

    @Test
    void synthesizesDatabaseDependencyNodeWithIdentityAttributes() {
        final OTelApmServiceMapProcessor proc = newFlushingProcessor("db-dep-");
        try {
            final Span dbClient = createMockSpanWithIds("product-catalog", "query", "SPAN_KIND_CLIENT",
                    "1111111111111111", "", "aaaaaaaaaaaaaaaa");
            final Map<String, Object> attrs = new HashMap<>();
            attrs.put("db.system.name", "postgresql");
            attrs.put("db.namespace", "otel");
            attrs.put("server.address", "postgresql");
            attrs.put("server.port", 5432);
            when(dbClient.getAttributes()).thenReturn(attrs);

            final List<Event> serviceMap = flushServiceMapEvents(proc,
                    Collections.singletonList(new Record<>(dbClient)));

            final Event dbNode = serviceMap.stream()
                    .filter(e -> "database".equals(e.get("targetNode/type", String.class)))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Expected a database dependency node"));
            final Map depAttrs = dbNode.get("targetNode/dependencyAttributes", Map.class);
            assertThat(depAttrs.get("db.system.name"), equalTo("postgresql"));
            assertThat(depAttrs.get("server.port"), equalTo("5432"));
        } finally {
            proc.shutdown();
        }
    }

    @Test
    void synthesizesMessagingBrokerNodeWithIdentityAttributes() {
        final OTelApmServiceMapProcessor proc = newFlushingProcessor("msg-dep-");
        try {
            final Span producer = createMockSpanWithIds("checkout", "publish orders", "SPAN_KIND_PRODUCER",
                    "1111111111111111", "", "aaaaaaaaaaaaaaaa");
            final Map<String, Object> attrs = new HashMap<>();
            attrs.put("messaging.system", "kafka");
            attrs.put("messaging.destination.name", "orders");
            attrs.put("messaging.operation", "publish");
            when(producer.getAttributes()).thenReturn(attrs);

            final List<Event> serviceMap = flushServiceMapEvents(proc,
                    Collections.singletonList(new Record<>(producer)));

            final Event brokerNode = serviceMap.stream()
                    .filter(e -> "messaging".equals(e.get("targetNode/type", String.class)))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Expected a messaging broker node"));
            assertThat(brokerNode.get("targetNode/keyAttributes/name", String.class), equalTo("kafka:orders"));
            final Map depAttrs = brokerNode.get("targetNode/dependencyAttributes", Map.class);
            assertThat(depAttrs.get("messaging.system"), equalTo("kafka"));
            assertThat(depAttrs.get("messaging.destination.name"), equalTo("orders"));
        } finally {
            proc.shutdown();
        }
    }

    @Test
    void suppressesUnresolvedExternalDependency() {
        final OTelApmServiceMapProcessor proc = newFlushingProcessor("supp-dep-");
        try {
            // http.request.method classifies the target as external, but with no identifiable peer
            // the derived name resolves to UnknownRemoteService and must not produce a node.
            final Span client = createMockSpanWithIds("frontend", "GET", "SPAN_KIND_CLIENT",
                    "1111111111111111", "", "aaaaaaaaaaaaaaaa");
            final Map<String, Object> attrs = new HashMap<>();
            attrs.put("http.request.method", "GET");
            when(client.getAttributes()).thenReturn(attrs);

            final List<Event> serviceMap = flushServiceMapEvents(proc,
                    Collections.singletonList(new Record<>(client)));

            assertTrue(serviceMap.stream()
                    .noneMatch(e -> "external".equals(e.get("targetNode/type", String.class))));
        } finally {
            proc.shutdown();
        }
    }

    @Test
    void clientWithChildServerYieldsServiceTargetAndNoDependencyAttributes() {
        final OTelApmServiceMapProcessor proc = newFlushingProcessor("svc-edge-");
        try {
            // CLIENT to an instrumented service (child SERVER span present). Even with external-ish
            // attributes on the client, the target must be the traced service, with NO
            // dependencyAttributes — the regression guard against classifying a real service as external.
            final Span client = createMockSpanWithIds("frontend", "GET /cart", "SPAN_KIND_CLIENT",
                    "1111111111111111", "", "aaaaaaaaaaaaaaaa");
            final Map<String, Object> attrs = new HashMap<>();
            attrs.put("http.request.method", "GET");
            attrs.put("server.address", "cart");
            when(client.getAttributes()).thenReturn(attrs);
            final Span server = createMockSpanWithIds("cart", "GET /cart", "SPAN_KIND_SERVER",
                    "2222222222222222", "1111111111111111", "aaaaaaaaaaaaaaaa");

            final List<Event> serviceMap = flushServiceMapEvents(proc,
                    Arrays.asList(new Record<>(client), new Record<>(server)));

            final Event edge = serviceMap.stream()
                    .filter(e -> "cart".equals(e.get("targetNode/keyAttributes/name", String.class)))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Expected a frontend -> cart service edge"));
            assertThat(edge.get("targetNode/type", String.class), equalTo("service"));
            // @JsonInclude(NON_EMPTY): a service node must not carry dependencyAttributes at all.
            assertNull(edge.get("targetNode/dependencyAttributes", Map.class));
        } finally {
            proc.shutdown();
        }
    }

    @Test
    void synthesizesConsumerBrokerToServiceEdge() {
        final OTelApmServiceMapProcessor proc = newFlushingProcessor("consumer-");
        try {
            final Span consumer = createMockSpanWithIds("shipping", "receive orders", "SPAN_KIND_CONSUMER",
                    "1111111111111111", "", "aaaaaaaaaaaaaaaa");
            final Map<String, Object> attrs = new HashMap<>();
            attrs.put("messaging.system", "kafka");
            attrs.put("messaging.destination.name", "orders");
            attrs.put("messaging.operation", "receive");
            when(consumer.getAttributes()).thenReturn(attrs);

            final List<Event> serviceMap = flushServiceMapEvents(proc,
                    Collections.singletonList(new Record<>(consumer)));

            // CONSUMER emits broker -> service: source is the broker, target is the service.
            final Event edge = serviceMap.stream()
                    .filter(e -> "kafka:orders".equals(e.get("sourceNode/keyAttributes/name", String.class)))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Expected a kafka:orders -> shipping edge"));
            assertThat(edge.get("sourceNode/type", String.class), equalTo("messaging"));
            assertThat(edge.get("targetNode/keyAttributes/name", String.class), equalTo("shipping"));
        } finally {
            proc.shutdown();
        }
    }

    // ---- Review round 2: rollout gate, cardinality, naming and double-count fixes ----

    private static Span withAttributes(final Span span, final Map<String, Object> attributes) {
        when(span.getAttributes()).thenReturn(attributes);
        return span;
    }

    private static Span withEnvironment(final Span span, final String environment) {
        final Map<String, Object> resourceAttributes = new HashMap<>();
        resourceAttributes.put("deployment.environment", environment);
        final Map<String, Object> resource = new HashMap<>();
        resource.put("attributes", resourceAttributes);
        when(span.getResource()).thenReturn(resource);
        return span;
    }

    private static List<JacksonSum> requestMetrics(final Collection<Record<Event>> records) {
        return records.stream()
                .map(Record::getData)
                .filter(e -> e instanceof JacksonSum)
                .map(e -> (JacksonSum) e)
                .filter(m -> "request".equals(m.getName()))
                .collect(Collectors.toList());
    }

    private static double requestCount(final Collection<Record<Event>> records, final String service,
                                       final String remoteService) {
        return requestMetrics(records).stream()
                .filter(m -> service.equals(m.getAttributes().get("service"))
                        && remoteService.equals(m.getAttributes().get("remoteService")))
                .mapToDouble(JacksonSum::getValue)
                .sum();
    }

    private static Set<String> targetNamesOfType(final List<Event> serviceMap, final String type) {
        return serviceMap.stream()
                .filter(e -> type.equals(e.get("targetNode/type", String.class)))
                .map(e -> e.get("targetNode/keyAttributes/name", String.class))
                .collect(Collectors.toSet());
    }

    private Map<String, Object> dbAttributes(final String system, final String host) {
        final Map<String, Object> attrs = new HashMap<>();
        attrs.put("db.system.name", system);
        attrs.put("server.address", host);
        return attrs;
    }

    private Map<String, Object> kafkaAttributes(final String operation) {
        final Map<String, Object> attrs = new HashMap<>();
        attrs.put("messaging.system", "kafka");
        attrs.put("messaging.destination.name", "orders");
        if (operation != null) {
            attrs.put("messaging.operation", operation);
        }
        return attrs;
    }

    private List<Record<Event>> dependencyTraceBatch() {
        final Span dbClient = withAttributes(createMockSpanWithIds("checkout", "query", "SPAN_KIND_CLIENT",
                "1111111111111111", "", "aaaaaaaaaaaaaaaa"), dbAttributes("postgresql", "orders-db"));
        final Span producer = withAttributes(createMockSpanWithIds("checkout", "orders publish", "SPAN_KIND_PRODUCER",
                "2222222222222222", "", "aaaaaaaaaaaaaaaa"), kafkaAttributes("publish"));
        final Map<String, Object> httpAttrs = new HashMap<>();
        httpAttrs.put("http.request.method", "GET");
        httpAttrs.put("url.full", "https://api.example.com/v1/charges");
        final Span httpClient = withAttributes(createMockSpanWithIds("checkout", "GET", "SPAN_KIND_CLIENT",
                "3333333333333333", "", "aaaaaaaaaaaaaaaa"), httpAttrs);
        return Arrays.asList(new Record<>(dbClient), new Record<>(producer), new Record<>(httpClient));
    }

    @Test
    void dependencyNodes_enabledByDefault_emitsDependencyNodes() {
        final OTelApmServiceMapProcessor proc = newFlushingProcessor("gate-default-", new DependencyNodesConfig());
        try {
            final Collection<Record<Event>> emitted = flushAll(proc, dependencyTraceBatch());

            assertThat(targetNamesOfType(serviceMapEvents(emitted), "database"), equalTo(Set.of("postgresql:orders-db")));
            assertThat(requestCount(emitted, "checkout", "kafka:orders"), equalTo(1.0));
        } finally {
            proc.shutdown();
        }
    }

    @Test
    void dependencyNodes_explicitlyDisabled_emitsNoDependencyNodesOrMetrics() {
        final OTelApmServiceMapProcessor proc = newFlushingProcessor("gate-off-",
                new DependencyNodesConfig(false, DependencyNodesConfig.DEFAULT_MAX_DEPENDENCIES_PER_SERVICE,
                        DependencyNodesConfig.DEFAULT_MAX_REMOTE_OPERATIONS_PER_SERVICE, Collections.emptyList()));
        try {
            final Collection<Record<Event>> emitted = flushAll(proc, dependencyTraceBatch());

            // With the option off, output matches the previous release: CLIENT/PRODUCER spans without a
            // downstream SERVER span produce nothing.
            assertThat(serviceMapEvents(emitted).size(), equalTo(0));
            assertThat(requestMetrics(emitted).size(), equalTo(0));
        } finally {
            proc.shutdown();
        }
    }

    // The plugin constructor runs on the system clock, so its wiring is checked through the overflow
    // counters, which are registered only when dependency nodes are enabled.
    private OTelApmServiceMapProcessor newPluginConstructedProcessor(final DependencyNodesConfig dependencyNodesConfig) {
        when(config.getDbPath()).thenReturn(new File(tempDir, "gate-plugin-" + System.nanoTime()).getAbsolutePath());
        when(config.getDependencyNodes()).thenReturn(dependencyNodesConfig);
        return new OTelApmServiceMapProcessor(config, pluginMetrics, eventFactory, pipelineDescription);
    }

    @Test
    void dependencyNodes_pluginConstructorWithoutDependencyNodesConfig_isEnabled() {
        final OTelApmServiceMapProcessor proc = newPluginConstructedProcessor(null);
        try {
            verify(pluginMetrics).counter(OTelApmServiceMapProcessor.DEPENDENCY_CALLS_OVERFLOWED_METRIC);
            verify(pluginMetrics).counter(OTelApmServiceMapProcessor.REMOTE_OPERATION_CALLS_OVERFLOWED_METRIC);
        } finally {
            proc.shutdown();
        }
    }

    @Test
    void dependencyNodes_pluginConstructorExplicitlyDisabled_registersNoOverflowCounters() {
        final OTelApmServiceMapProcessor proc = newPluginConstructedProcessor(
                new DependencyNodesConfig(false, DependencyNodesConfig.DEFAULT_MAX_DEPENDENCIES_PER_SERVICE,
                        DependencyNodesConfig.DEFAULT_MAX_REMOTE_OPERATIONS_PER_SERVICE, Collections.emptyList()));
        try {
            verify(pluginMetrics, never()).counter(OTelApmServiceMapProcessor.DEPENDENCY_CALLS_OVERFLOWED_METRIC);
            verify(pluginMetrics, never()).counter(OTelApmServiceMapProcessor.REMOTE_OPERATION_CALLS_OVERFLOWED_METRIC);
        } finally {
            proc.shutdown();
        }
    }

    @Test
    void dependencyNodes_enabled_registersOverflowCountersNamedForCollapsedCalls() {
        final OTelApmServiceMapProcessor proc = newPluginConstructedProcessor(enabledDependencyNodes());
        try {
            verify(pluginMetrics).counter("dependencyCallsOverflowed");
            verify(pluginMetrics).counter("dependencyRemoteOperationCallsOverflowed");
            verify(pluginMetrics, never()).gauge(eq("dependencyCallsOverflowed"), any(AtomicLong.class));
            verify(pluginMetrics, never()).gauge(eq("dependencyRemoteOperationCallsOverflowed"), any(AtomicLong.class));
            verify(pluginMetrics, never()).gauge(eq("dependencyCallsOverflowed"), any(), any());
            verify(pluginMetrics, never()).gauge(eq("dependencyRemoteOperationCallsOverflowed"), any(), any());
        } finally {
            proc.shutdown();
        }
    }

    @Test
    void dependencyNodes_enabled_emitsDatabaseMessagingAndExternalNodes() {
        final OTelApmServiceMapProcessor proc = newFlushingProcessor("gate-on-");
        try {
            final Collection<Record<Event>> emitted = flushAll(proc, dependencyTraceBatch());
            final List<Event> serviceMap = serviceMapEvents(emitted);

            assertThat(targetNamesOfType(serviceMap, "database"), equalTo(Set.of("postgresql:orders-db")));
            assertThat(targetNamesOfType(serviceMap, "messaging"), equalTo(Set.of("kafka:orders")));
            assertThat(targetNamesOfType(serviceMap, "external"), equalTo(Set.of("api.example.com")));
            assertThat(requestCount(emitted, "checkout", "kafka:orders"), equalTo(1.0));
        } finally {
            proc.shutdown();
        }
    }

    @Test
    void messagingSeries_carryASpanKindLabelSoProducersAndConsumersCanBeSplit() {
        final OTelApmServiceMapProcessor proc = newFlushingProcessor("span-kind-label-",
                new DependencyNodesConfig(true, 100, 100, Collections.emptyList()));
        try {
            final Span producer = withAttributes(createMockSpanWithIds("checkout", "orders publish", "SPAN_KIND_PRODUCER",
                    "1111111111111111", "", "aaaaaaaaaaaaaaaa"), kafkaAttributes("publish"));
            final Span consumer = withAttributes(createMockSpanWithIds("shipping", "orders receive", "SPAN_KIND_CONSUMER",
                    "2222222222222222", "1111111111111111", "aaaaaaaaaaaaaaaa"), kafkaAttributes("receive"));
            final Span dbCall = withAttributes(createMockSpanWithIds("checkout", "query", "SPAN_KIND_CLIENT",
                    "3333333333333333", "", "bbbbbbbbbbbbbbbb"), dbAttributes("postgresql", "orders-db"));

            final List<JacksonSum> requests = requestMetrics(
                    flushAll(proc, Arrays.asList(new Record<>(producer), new Record<>(consumer), new Record<>(dbCall))));

            final Map<String, Object> producerLabels = requests.stream()
                    .filter(m -> "checkout".equals(m.getAttributes().get("service"))
                            && "kafka:orders".equals(m.getAttributes().get("remoteService")))
                    .findFirst().orElseThrow(() -> new AssertionError("Expected the producer series")).getAttributes();
            final Map<String, Object> consumerLabels = requests.stream()
                    .filter(m -> "shipping".equals(m.getAttributes().get("service"))
                            && "kafka:orders".equals(m.getAttributes().get("remoteService")))
                    .findFirst().orElseThrow(() -> new AssertionError("Expected the consumer series")).getAttributes();
            final Map<String, Object> dbLabels = requests.stream()
                    .filter(m -> "postgresql:orders-db".equals(m.getAttributes().get("remoteService")))
                    .findFirst().orElseThrow(() -> new AssertionError("Expected the database series")).getAttributes();

            assertThat(producerLabels.get("spanKind"), equalTo("PRODUCER"));
            assertThat(consumerLabels.get("spanKind"), equalTo("CONSUMER"));
            // CLIENT series keep their previous label set.
            assertTrue(!dbLabels.containsKey("spanKind"));
        } finally {
            proc.shutdown();
        }
    }

    @Test
    void dependencyCap_collapsesDependenciesPastTheCapIntoOverflowNode() {
        final OTelApmServiceMapProcessor proc = newFlushingProcessor("cap-deps-",
                new DependencyNodesConfig(true, 1, 100, Collections.emptyList()));
        try {
            final Span first = withAttributes(createMockSpanWithIds("checkout", "query", "SPAN_KIND_CLIENT",
                    "1111111111111111", "", "aaaaaaaaaaaaaaaa"), dbAttributes("postgresql", "orders-db"));
            final Span second = withAttributes(createMockSpanWithIds("checkout", "query", "SPAN_KIND_CLIENT",
                    "2222222222222222", "", "aaaaaaaaaaaaaaaa"), dbAttributes("mysql", "users-db"));

            final Collection<Record<Event>> emitted = flushAll(proc, Arrays.asList(new Record<>(first), new Record<>(second)));
            final Set<String> names = targetNamesOfType(serviceMapEvents(emitted), "database");

            assertThat(names.size(), equalTo(2));
            assertTrue(names.contains("OtherDatabase"));
            assertThat(requestCount(emitted, "checkout", "OtherDatabase"), equalTo(1.0));
            verify(dependencyCallsOverflowedCounter, times(1)).increment();
        } finally {
            proc.shutdown();
        }
    }

    @Test
    void dependencyCap_admitsTheSameDependenciesWhateverTheTraceOrder() {
        final List<String> hosts = List.of("a-db", "a-db", "a-db", "b-db", "b-db", "c-db");
        for (int rotation = 0; rotation < hosts.size(); rotation++) {
            final OTelApmServiceMapProcessor proc = newFlushingProcessor("cap-order-" + rotation + "-",
                    new DependencyNodesConfig(true, 2, 100, Collections.emptyList()));
            try {
                final List<Record<Event>> batch = new ArrayList<>();
                for (int i = 0; i < hosts.size(); i++) {
                    final String traceId = String.format("1%015x", (i + rotation) % hosts.size());
                    batch.add(new Record<>(withAttributes(createMockSpanWithIds("checkout", "query", "SPAN_KIND_CLIENT",
                            String.format("%016x", i + 1), "", traceId), dbAttributes("postgresql", hosts.get(i)))));
                }

                final Collection<Record<Event>> emitted = flushAll(proc, batch);

                assertThat("rotation " + rotation, targetNamesOfType(serviceMapEvents(emitted), "database"),
                        equalTo(Set.of("postgresql:a-db", "postgresql:b-db", "OtherDatabase")));
                assertThat("rotation " + rotation, requestCount(emitted, "checkout", "OtherDatabase"), equalTo(1.0));
            } finally {
                proc.shutdown();
            }
        }
    }

    @Test
    void dependencyCap_overflowedDependencyOperationsDoNotConsumeTheOperationBudget() {
        final OTelApmServiceMapProcessor proc = newFlushingProcessor("cap-overflow-ops-",
                new DependencyNodesConfig(true, 1, 1, Collections.emptyList()));
        try {
            final List<Record<Event>> batch = new ArrayList<>();
            final List<String> hosts = List.of("a-db", "a-db", "b-db");
            final List<String> operations = List.of("SELECT", "SELECT", "INSERT");
            for (int i = 0; i < hosts.size(); i++) {
                final Map<String, Object> attributes = dbAttributes("postgresql", hosts.get(i));
                attributes.put("db.operation.name", operations.get(i));
                batch.add(new Record<>(withAttributes(createMockSpanWithIds("checkout", "query", "SPAN_KIND_CLIENT",
                        String.format("%016x", i + 1), "", String.format("1%015x", i)), attributes)));
            }

            final Set<String> edges = serviceMapEvents(flushAll(proc, batch)).stream()
                    .filter(e -> "database".equals(e.get("targetNode/type", String.class)))
                    .map(e -> e.get("targetNode/keyAttributes/name", String.class) + " " + e.get("targetOperation/name", String.class))
                    .collect(Collectors.toSet());

            assertThat(edges, equalTo(Set.of("postgresql:a-db SELECT", "OtherDatabase OtherRemoteOperation")));
            verify(remoteOperationCallsOverflowedCounter, never()).increment();
        } finally {
            proc.shutdown();
        }
    }

    @Test
    void dependencyCap_messagingDestinationsPastTheCap_collapseIntoOverflowBroker() {
        final OTelApmServiceMapProcessor proc = newFlushingProcessor("cap-brokers-",
                new DependencyNodesConfig(true, 1, 100, Collections.emptyList()));
        try {
            final List<String> destinations = List.of("payments", "orders", "orders");
            final List<Record<Event>> batch = new ArrayList<>();
            for (int i = 0; i < destinations.size(); i++) {
                final Map<String, Object> attributes = kafkaAttributes("receive");
                attributes.put("messaging.destination.name", destinations.get(i));
                batch.add(new Record<>(withAttributes(createMockSpanWithIds("shipping", "receive", "SPAN_KIND_CONSUMER",
                        String.format("%016x", i + 1), "", String.format("1%015x", i)), attributes)));
            }

            final Collection<Record<Event>> emitted = flushAll(proc, batch);
            final Set<String> edges = serviceMapEvents(emitted).stream()
                    .filter(e -> "messaging".equals(e.get("sourceNode/type", String.class)))
                    .map(e -> e.get("sourceNode/keyAttributes/name", String.class) + " " + e.get("sourceOperation/name", String.class))
                    .collect(Collectors.toSet());

            assertThat(edges, equalTo(Set.of("kafka:orders receive", "OtherMessaging OtherRemoteOperation")));
            verify(dependencyCallsOverflowedCounter, times(1)).increment();
            verify(remoteOperationCallsOverflowedCounter, never()).increment();
        } finally {
            proc.shutdown();
        }
    }

    @Test
    void dependencyCap_realDependencyNamedLikeTheOverflowBucket_doesNotCountOverflowedCallsAsOperationOverflows() {
        final OTelApmServiceMapProcessor proc = newFlushingProcessor("cap-literal-other-",
                new DependencyNodesConfig(true, 1, 1, Collections.emptyList()));
        try {
            final List<Record<Event>> batch = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                final Map<String, Object> attributes = dbAttributes("postgresql", i < 2 ? "a-db" : "b-db");
                attributes.put("db.operation.name", i < 2 ? "SELECT" : "INSERT");
                if (i < 2) {
                    attributes.put("peer.service", "OtherDatabase");
                }
                batch.add(new Record<>(withAttributes(createMockSpanWithIds("checkout", "query", "SPAN_KIND_CLIENT",
                        String.format("%016x", i + 1), "", String.format("1%015x", i)), attributes)));
            }

            final List<Event> databaseEdges = serviceMapEvents(flushAll(proc, batch)).stream()
                    .filter(e -> "database".equals(e.get("targetNode/type", String.class)))
                    .collect(Collectors.toList());
            final Set<String> edges = databaseEdges.stream()
                    .map(e -> e.get("targetNode/keyAttributes/name", String.class) + " " + e.get("targetOperation/name", String.class))
                    .collect(Collectors.toSet());

            assertThat(edges, equalTo(Set.of("OtherDatabase query", "OtherDatabase OtherRemoteOperation")));
            final Event admittedEdge = databaseEdges.stream()
                    .filter(e -> "query".equals(e.get("targetOperation/name", String.class)))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Expected the admitted OtherDatabase edge"));
            assertThat(admittedEdge.get("targetNode/dependencyAttributes/db.system.name", String.class), equalTo("postgresql"));
            verify(dependencyCallsOverflowedCounter, times(1)).increment();
            verify(remoteOperationCallsOverflowedCounter, never()).increment();
        } finally {
            proc.shutdown();
        }
    }

    @Test
    void dependencyCap_overflowsInSuccessiveWindows_accumulateOnTheCounter() {
        final AtomicReference<Instant> now = new AtomicReference<>(testTime);
        when(clock.instant()).thenAnswer(invocation -> now.get());
        final OTelApmServiceMapProcessor proc = newProcessorCapturingEvents("cap-windows-",
                new DependencyNodesConfig(true, 1, 100, Collections.emptyList()));
        try {
            for (int window = 0; window < 4; window++) {
                final List<Record<Event>> batch = new ArrayList<>();
                if (window < 2) {
                    for (int i = 0; i < 3; i++) {
                        batch.add(new Record<>(withAttributes(createMockSpanWithIds("checkout", "query", "SPAN_KIND_CLIENT",
                                String.format("%016x", window * 10 + i + 1), "", String.format("1%015x", window * 10 + i)),
                                dbAttributes("postgresql", i < 2 ? "a-db" : "b-db"))));
                    }
                }
                proc.doExecute(batch);
                now.set(now.get().plusSeconds(65));
            }

            verify(dependencyCallsOverflowedCounter, times(2)).increment();
            verify(remoteOperationCallsOverflowedCounter, never()).increment();
        } finally {
            proc.shutdown();
        }
    }

    @Test
    void remoteOperationCap_collapsesOperationsPastTheCapIntoOverflowOperation() {
        final OTelApmServiceMapProcessor proc = newFlushingProcessor("cap-ops-",
                new DependencyNodesConfig(true, 100, 1, Collections.emptyList()));
        try {
            final Map<String, Object> select = dbAttributes("postgresql", "orders-db");
            select.put("db.operation.name", "SELECT");
            final Map<String, Object> insert = dbAttributes("postgresql", "orders-db");
            insert.put("db.operation.name", "INSERT");
            final Span first = withAttributes(createMockSpanWithIds("checkout", "query", "SPAN_KIND_CLIENT",
                    "1111111111111111", "", "aaaaaaaaaaaaaaaa"), select);
            final Span second = withAttributes(createMockSpanWithIds("checkout", "query", "SPAN_KIND_CLIENT",
                    "2222222222222222", "", "aaaaaaaaaaaaaaaa"), insert);

            final List<Event> serviceMap = serviceMapEvents(
                    flushAll(proc, Arrays.asList(new Record<>(first), new Record<>(second))));
            final Set<String> operations = serviceMap.stream()
                    .filter(e -> "database".equals(e.get("targetNode/type", String.class)))
                    .map(e -> e.get("targetOperation/name", String.class))
                    .collect(Collectors.toSet());

            assertThat(operations.size(), equalTo(2));
            assertTrue(operations.contains("OtherRemoteOperation"));
            verify(remoteOperationCallsOverflowedCounter, times(1)).increment();
        } finally {
            proc.shutdown();
        }
    }

    @Test
    void externalCall_withoutHttpMethod_usesFirstPathSegmentAndNeverTheRawUrl() {
        final OTelApmServiceMapProcessor proc = newFlushingProcessor("raw-url-");
        try {
            final Map<String, Object> attrs = new HashMap<>();
            attrs.put("url.full", "https://user:pass@api.example.com/users/alice@example.com/orders");
            final Span client = withAttributes(createMockSpanWithIds("frontend", "call", "SPAN_KIND_CLIENT",
                    "1111111111111111", "", "aaaaaaaaaaaaaaaa"), attrs);

            final Collection<Record<Event>> emitted = flushAll(proc, Collections.singletonList(new Record<>(client)));
            final Event edge = serviceMapEvents(emitted).stream()
                    .filter(e -> "external".equals(e.get("targetNode/type", String.class)))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Expected an external dependency edge"));

            assertThat(edge.get("targetOperation/name", String.class), equalTo("/users"));
            assertTrue(requestMetrics(emitted).stream()
                    .noneMatch(m -> String.valueOf(m.getAttributes().get("remoteOperation")).contains("pass")));
        } finally {
            proc.shutdown();
        }
    }

    @Test
    void brokerNode_isSharedBetweenProducerAndConsumerInDifferentEnvironments() {
        final OTelApmServiceMapProcessor proc = newFlushingProcessor("broker-env-");
        try {
            final Span producer = withEnvironment(withAttributes(createMockSpanWithIds("checkout", "orders publish",
                    "SPAN_KIND_PRODUCER", "1111111111111111", "", "aaaaaaaaaaaaaaaa"), kafkaAttributes("publish")),
                    "eks:prod/ns-a");
            final Span consumer = withEnvironment(withAttributes(createMockSpanWithIds("shipping", "orders process",
                    "SPAN_KIND_CONSUMER", "2222222222222222", "1111111111111111", "aaaaaaaaaaaaaaaa"),
                    kafkaAttributes("process")), "eks:prod/ns-b");

            final List<Event> serviceMap = flushServiceMapEvents(proc,
                    Arrays.asList(new Record<>(producer), new Record<>(consumer)));
            final Event producerEdge = serviceMap.stream()
                    .filter(e -> "messaging".equals(e.get("targetNode/type", String.class)))
                    .findFirst().orElseThrow(() -> new AssertionError("Expected checkout -> kafka:orders"));
            final Event consumerEdge = serviceMap.stream()
                    .filter(e -> "messaging".equals(e.get("sourceNode/type", String.class)))
                    .findFirst().orElseThrow(() -> new AssertionError("Expected kafka:orders -> shipping"));

            assertThat(producerEdge.get("targetNode/keyAttributes/environment", String.class),
                    equalTo(consumerEdge.get("sourceNode/keyAttributes/environment", String.class)));
            assertThat(producerEdge.get("targetNode/keyAttributes", Map.class),
                    equalTo(consumerEdge.get("sourceNode/keyAttributes", Map.class)));
        } finally {
            proc.shutdown();
        }
    }

    @Test
    void producerEdge_usesParentServerOperationAsSourceOperation() {
        final OTelApmServiceMapProcessor proc = newFlushingProcessor("producer-op-");
        try {
            final Span server = createMockSpanWithIds("checkout", "POST /checkout", "SPAN_KIND_SERVER",
                    "1111111111111111", "", "aaaaaaaaaaaaaaaa");
            final Span producer = withAttributes(createMockSpanWithIds("checkout", "orders publish", "SPAN_KIND_PRODUCER",
                    "2222222222222222", "1111111111111111", "aaaaaaaaaaaaaaaa"), kafkaAttributes("publish"));

            final Collection<Record<Event>> emitted = flushAll(proc, Arrays.asList(new Record<>(server), new Record<>(producer)));
            final Event edge = serviceMapEvents(emitted).stream()
                    .filter(e -> "messaging".equals(e.get("targetNode/type", String.class)))
                    .findFirst().orElseThrow(() -> new AssertionError("Expected checkout -> kafka:orders"));

            assertThat(edge.get("sourceOperation/name", String.class), equalTo("POST /checkout"));
            assertTrue(requestMetrics(emitted).stream()
                    .filter(m -> "kafka:orders".equals(m.getAttributes().get("remoteService")))
                    .allMatch(m -> "POST /checkout".equals(m.getAttributes().get("operation"))));
        } finally {
            proc.shutdown();
        }
    }

    @Test
    void nestedClientSpans_synthesizeOnlyTheOutermostDependency() {
        final OTelApmServiceMapProcessor proc = newFlushingProcessor("nested-client-");
        try {
            final Map<String, Object> sdkAttrs = new HashMap<>();
            sdkAttrs.put("rpc.system", "aws-api");
            sdkAttrs.put("rpc.service", "DynamoDb");
            sdkAttrs.put("rpc.method", "GetItem");
            final Span sdkClient = withAttributes(createMockSpanWithIds("checkout", "DynamoDB.GetItem", "SPAN_KIND_CLIENT",
                    "1111111111111111", "", "aaaaaaaaaaaaaaaa"), sdkAttrs);
            final Map<String, Object> httpAttrs = new HashMap<>();
            httpAttrs.put("http.request.method", "POST");
            httpAttrs.put("server.address", "dynamodb.us-east-1.amazonaws.com");
            httpAttrs.put("server.port", 443);
            final Span transportClient = withAttributes(createMockSpanWithIds("checkout", "POST", "SPAN_KIND_CLIENT",
                    "2222222222222222", "1111111111111111", "aaaaaaaaaaaaaaaa"), httpAttrs);

            final Collection<Record<Event>> emitted = flushAll(proc,
                    Arrays.asList(new Record<>(sdkClient), new Record<>(transportClient)));

            assertThat(targetNamesOfType(serviceMapEvents(emitted), "external"), equalTo(Set.of("AWS::DynamoDB")));
            assertThat(requestCount(emitted, "checkout", "dynamodb.us-east-1.amazonaws.com"), equalTo(0.0));
        } finally {
            proc.shutdown();
        }
    }

    @Test
    void nestedClientSpans_whoseInnerCallReachesATracedService_synthesizeNoDependency() {
        final OTelApmServiceMapProcessor proc = newFlushingProcessor("nested-svc-");
        try {
            final Map<String, Object> logicalAttrs = new HashMap<>();
            logicalAttrs.put("peer.service", "cart-api");
            final Span logicalClient = withAttributes(createMockSpanWithIds("frontend", "cart.get", "SPAN_KIND_CLIENT",
                    "1111111111111111", "", "aaaaaaaaaaaaaaaa"), logicalAttrs);
            final Map<String, Object> httpAttrs = new HashMap<>();
            httpAttrs.put("http.request.method", "GET");
            httpAttrs.put("server.address", "cart");
            final Span transportClient = withAttributes(createMockSpanWithIds("frontend", "GET", "SPAN_KIND_CLIENT",
                    "2222222222222222", "1111111111111111", "aaaaaaaaaaaaaaaa"), httpAttrs);
            final Span server = createMockSpanWithIds("cart", "GET /cart", "SPAN_KIND_SERVER",
                    "3333333333333333", "2222222222222222", "aaaaaaaaaaaaaaaa");

            final List<Event> serviceMap = flushServiceMapEvents(proc,
                    Arrays.asList(new Record<>(logicalClient), new Record<>(transportClient), new Record<>(server)));

            assertThat(targetNamesOfType(serviceMap, "external"), equalTo(Collections.emptySet()));
            assertThat(targetNamesOfType(serviceMap, "service"), equalTo(Set.of("cart")));
        } finally {
            proc.shutdown();
        }
    }

    @Test
    void clientToInstrumentedServiceWithMissingServerSpan_isNotSynthesizedAsExternal() {
        final OTelApmServiceMapProcessor proc = newFlushingProcessor("ghost-");
        try {
            // Trace A proves "checkout" is an instrumented service in the windows.
            final Span clientA = createMockSpanWithIds("frontend", "GET /checkout", "SPAN_KIND_CLIENT",
                    "1111111111111111", "", "aaaaaaaaaaaaaaaa");
            final Span serverA = createMockSpanWithIds("checkout", "GET /checkout", "SPAN_KIND_SERVER",
                    "2222222222222222", "1111111111111111", "aaaaaaaaaaaaaaaa");
            // Trace B: the child SERVER span of checkout was not captured.
            final Map<String, Object> attrs = new HashMap<>();
            attrs.put("http.request.method", "GET");
            attrs.put("server.address", "checkout");
            attrs.put("server.port", 8080);
            final Span clientB = withAttributes(createMockSpanWithIds("frontend", "GET", "SPAN_KIND_CLIENT",
                    "3333333333333333", "", "bbbbbbbbbbbbbbbb"), attrs);
            final Map<String, Object> peerAttrs = new HashMap<>();
            peerAttrs.put("peer.service", "checkout");
            final Span clientC = withAttributes(createMockSpanWithIds("frontend", "call", "SPAN_KIND_CLIENT",
                    "4444444444444444", "", "cccccccccccccccc"), peerAttrs);

            final List<Event> serviceMap = flushServiceMapEvents(proc, Arrays.asList(
                    new Record<>(clientA), new Record<>(serverA), new Record<>(clientB), new Record<>(clientC)));

            assertThat(targetNamesOfType(serviceMap, "external"), equalTo(Collections.emptySet()));
        } finally {
            proc.shutdown();
        }
    }

    @Test
    void receiveAndProcessConsumerSpans_countTheMessageOnce() {
        final OTelApmServiceMapProcessor proc = newFlushingProcessor("recv-proc-");
        try {
            final Span producer = withAttributes(createMockSpanWithIds("checkout", "orders publish", "SPAN_KIND_PRODUCER",
                    "1111111111111111", "", "aaaaaaaaaaaaaaaa"), kafkaAttributes("publish"));
            final Span receive = withAttributes(createMockSpanWithIds("shipping", "orders receive", "SPAN_KIND_CONSUMER",
                    "2222222222222222", "1111111111111111", "aaaaaaaaaaaaaaaa"), kafkaAttributes("receive"));
            final Span process = withAttributes(createMockSpanWithIds("shipping", "orders process", "SPAN_KIND_CONSUMER",
                    "3333333333333333", "1111111111111111", "aaaaaaaaaaaaaaaa"), kafkaAttributes("process"));

            final Collection<Record<Event>> emitted = flushAll(proc,
                    Arrays.asList(new Record<>(producer), new Record<>(receive), new Record<>(process)));

            assertThat(requestCount(emitted, "shipping", "kafka:orders"), equalTo(1.0));
            final long consumerEdges = serviceMapEvents(emitted).stream()
                    .filter(e -> "messaging".equals(e.get("sourceNode/type", String.class)))
                    .count();
            assertThat(consumerEdges, equalTo(1L));
        } finally {
            proc.shutdown();
        }
    }

    @Test
    void rootConsumerSpansWithEmptyParentSpanId_areNotMergedAsOneMessage() {
        final OTelApmServiceMapProcessor proc = newFlushingProcessor("root-consumers-");
        try {
            final Span receive = withAttributes(createMockSpanWithIds("shipping", "orders receive", "SPAN_KIND_CONSUMER",
                    "1111111111111111", "", "aaaaaaaaaaaaaaaa"), kafkaAttributes("receive"));
            final Span process = withAttributes(createMockSpanWithIds("shipping", "orders process", "SPAN_KIND_CONSUMER",
                    "2222222222222222", "", "aaaaaaaaaaaaaaaa"), kafkaAttributes("process"));

            final Collection<Record<Event>> emitted = flushAll(proc, Arrays.asList(new Record<>(receive), new Record<>(process)));

            // Root spans carry no parent, so they cannot be matched as one message and both are counted.
            assertThat(requestCount(emitted, "shipping", "kafka:orders"), equalTo(2.0));
        } finally {
            proc.shutdown();
        }
    }

    @Test
    void nestedConsumerSpanForSameDestination_countsTheMessageOnce() {
        final OTelApmServiceMapProcessor proc = newFlushingProcessor("nested-consumer-");
        try {
            final Span receive = withAttributes(createMockSpanWithIds("shipping", "orders receive", "SPAN_KIND_CONSUMER",
                    "1111111111111111", "", "aaaaaaaaaaaaaaaa"), kafkaAttributes("receive"));
            final Span process = withAttributes(createMockSpanWithIds("shipping", "orders process", "SPAN_KIND_CONSUMER",
                    "2222222222222222", "1111111111111111", "aaaaaaaaaaaaaaaa"), kafkaAttributes("process"));

            final Collection<Record<Event>> emitted = flushAll(proc, Arrays.asList(new Record<>(receive), new Record<>(process)));

            assertThat(requestCount(emitted, "shipping", "kafka:orders"), equalTo(1.0));
        } finally {
            proc.shutdown();
        }
    }

    @Test
    void legacyMessagingDestinationKey_producesBrokerEdge() {
        final OTelApmServiceMapProcessor proc = newFlushingProcessor("legacy-dest-");
        try {
            final Map<String, Object> attrs = new HashMap<>();
            attrs.put("messaging.system", "rabbitmq");
            attrs.put("messaging.destination", "invoices");
            final Span producer = withAttributes(createMockSpanWithIds("billing", "invoices send", "SPAN_KIND_PRODUCER",
                    "1111111111111111", "", "aaaaaaaaaaaaaaaa"), attrs);

            final List<Event> serviceMap = flushServiceMapEvents(proc, Collections.singletonList(new Record<>(producer)));

            assertThat(targetNamesOfType(serviceMap, "messaging"), equalTo(Set.of("rabbitmq:invoices")));
        } finally {
            proc.shutdown();
        }
    }

    @Test
    void dependencyCallWithoutParentServer_usesSameOperationInTopologyAndMetrics() {
        final OTelApmServiceMapProcessor proc = newFlushingProcessor("orphan-op-");
        try {
            final Span dbClient = withAttributes(createMockSpanWithIds("reporting", "nightly query", "SPAN_KIND_CLIENT",
                    "1111111111111111", "", "aaaaaaaaaaaaaaaa"), dbAttributes("postgresql", "orders-db"));

            final Collection<Record<Event>> emitted = flushAll(proc, Collections.singletonList(new Record<>(dbClient)));
            final Event edge = serviceMapEvents(emitted).stream()
                    .filter(e -> "database".equals(e.get("targetNode/type", String.class)))
                    .findFirst().orElseThrow(() -> new AssertionError("Expected reporting -> postgresql edge"));
            final Set<Object> metricOperations = requestMetrics(emitted).stream()
                    .map(m -> m.getAttributes().get("operation"))
                    .collect(Collectors.toSet());

            assertThat(edge.get("sourceOperation/name", String.class), equalTo("nightly query"));
            assertThat(metricOperations, equalTo(Set.of("nightly query")));
        } finally {
            proc.shutdown();
        }
    }

    @Test
    void dependencyCallUnderConsumerSpan_usesConsumerOperationAsSourceOperation() {
        final OTelApmServiceMapProcessor proc = newFlushingProcessor("consumer-op-");
        try {
            final Span consumer = withAttributes(createMockSpanWithIds("shipping", "orders process", "SPAN_KIND_CONSUMER",
                    "1111111111111111", "", "aaaaaaaaaaaaaaaa"), kafkaAttributes("process"));
            final Span dbClient = withAttributes(createMockSpanWithIds("shipping", "insert shipment", "SPAN_KIND_CLIENT",
                    "2222222222222222", "1111111111111111", "aaaaaaaaaaaaaaaa"), dbAttributes("postgresql", "ship-db"));

            final Collection<Record<Event>> emitted = flushAll(proc, Arrays.asList(new Record<>(consumer), new Record<>(dbClient)));
            final Event edge = serviceMapEvents(emitted).stream()
                    .filter(e -> "database".equals(e.get("targetNode/type", String.class)))
                    .findFirst().orElseThrow(() -> new AssertionError("Expected shipping -> postgresql edge"));

            assertThat(edge.get("sourceOperation/name", String.class), equalTo("orders process"));
            assertTrue(requestMetrics(emitted).stream()
                    .filter(m -> "postgresql:ship-db".equals(m.getAttributes().get("remoteService")))
                    .allMatch(m -> "orders process".equals(m.getAttributes().get("operation"))));
        } finally {
            proc.shutdown();
        }
    }

    @Test
    void transportClientUnderProducerOrReceiveConsumer_isNotSynthesized() {
        final OTelApmServiceMapProcessor proc = newFlushingProcessor("transport-msg-");
        try {
            final Map<String, Object> snsAttrs = new HashMap<>();
            snsAttrs.put("messaging.system", "aws_sns");
            snsAttrs.put("messaging.destination.name", "orders");
            snsAttrs.put("messaging.operation", "publish");
            final Span producer = withAttributes(createMockSpanWithIds("checkout", "orders publish", "SPAN_KIND_PRODUCER",
                    "1111111111111111", "", "aaaaaaaaaaaaaaaa"), snsAttrs);
            final Map<String, Object> snsHttp = new HashMap<>();
            snsHttp.put("http.request.method", "POST");
            snsHttp.put("url.full", "https://sns.us-east-1.amazonaws.com/");
            final Span producerTransport = withAttributes(createMockSpanWithIds("checkout", "POST", "SPAN_KIND_CLIENT",
                    "2222222222222222", "1111111111111111", "aaaaaaaaaaaaaaaa"), snsHttp);
            final Map<String, Object> sqsAttrs = new HashMap<>();
            sqsAttrs.put("messaging.system", "aws_sqs");
            sqsAttrs.put("messaging.destination.name", "shipments");
            sqsAttrs.put("messaging.operation", "receive");
            final Span receive = withAttributes(createMockSpanWithIds("shipping", "shipments receive", "SPAN_KIND_CONSUMER",
                    "3333333333333333", "", "aaaaaaaaaaaaaaaa"), sqsAttrs);
            final Map<String, Object> sqsHttp = new HashMap<>();
            sqsHttp.put("http.request.method", "POST");
            sqsHttp.put("url.full", "https://sqs.us-east-1.amazonaws.com/");
            final Span receiveTransport = withAttributes(createMockSpanWithIds("shipping", "POST", "SPAN_KIND_CLIENT",
                    "4444444444444444", "3333333333333333", "aaaaaaaaaaaaaaaa"), sqsHttp);

            final List<Event> serviceMap = flushServiceMapEvents(proc, Arrays.asList(new Record<>(producer),
                    new Record<>(producerTransport), new Record<>(receive), new Record<>(receiveTransport)));

            assertThat(targetNamesOfType(serviceMap, "external"), equalTo(Collections.emptySet()));
            assertThat(targetNamesOfType(serviceMap, "messaging"), equalTo(Set.of("aws_sns:orders")));
        } finally {
            proc.shutdown();
        }
    }

    @Test
    void producerWithoutMessagingOperation_usesSameBrokerOperationInTopologyAndMetrics() {
        final OTelApmServiceMapProcessor proc = newFlushingProcessor("producer-no-op-");
        try {
            final Span producer = withAttributes(createMockSpanWithIds("checkout", "orders send", "SPAN_KIND_PRODUCER",
                    "1111111111111111", "", "aaaaaaaaaaaaaaaa"), kafkaAttributes(null));

            final Collection<Record<Event>> emitted = flushAll(proc, Collections.singletonList(new Record<>(producer)));
            final Event edge = serviceMapEvents(emitted).stream()
                    .filter(e -> "messaging".equals(e.get("targetNode/type", String.class)))
                    .findFirst().orElseThrow(() -> new AssertionError("Expected checkout -> kafka:orders"));
            final Set<Object> metricRemoteOperations = requestMetrics(emitted).stream()
                    .map(m -> m.getAttributes().get("remoteOperation"))
                    .collect(Collectors.toSet());

            assertThat(edge.get("targetOperation/name", String.class), equalTo("orders send"));
            assertThat(metricRemoteOperations, equalTo(Set.of("orders send")));
        } finally {
            proc.shutdown();
        }
    }

    @Test
    void receiveAndProcessConsumerSpansForDifferentMessages_areBothCounted() {
        final OTelApmServiceMapProcessor proc = newFlushingProcessor("recv-proc-diff-");
        try {
            final Span producerA = withAttributes(createMockSpanWithIds("checkout", "orders publish", "SPAN_KIND_PRODUCER",
                    "1111111111111111", "", "aaaaaaaaaaaaaaaa"), kafkaAttributes("publish"));
            final Span producerB = withAttributes(createMockSpanWithIds("checkout", "orders publish", "SPAN_KIND_PRODUCER",
                    "2222222222222222", "", "aaaaaaaaaaaaaaaa"), kafkaAttributes("publish"));
            final Span receiveA = withAttributes(createMockSpanWithIds("shipping", "orders receive", "SPAN_KIND_CONSUMER",
                    "3333333333333333", "1111111111111111", "aaaaaaaaaaaaaaaa"), kafkaAttributes("receive"));
            final Span processB = withAttributes(createMockSpanWithIds("shipping", "orders process", "SPAN_KIND_CONSUMER",
                    "4444444444444444", "2222222222222222", "aaaaaaaaaaaaaaaa"), kafkaAttributes("process"));

            final Collection<Record<Event>> emitted = flushAll(proc, Arrays.asList(new Record<>(producerA),
                    new Record<>(producerB), new Record<>(receiveA), new Record<>(processB)));

            assertThat(requestCount(emitted, "shipping", "kafka:orders"), equalTo(2.0));
        } finally {
            proc.shutdown();
        }
    }

    private static DependencyNodesConfig enabledDependencyNodes() {
        return new DependencyNodesConfig(true, DependencyNodesConfig.DEFAULT_MAX_DEPENDENCIES_PER_SERVICE,
                DependencyNodesConfig.DEFAULT_MAX_REMOTE_OPERATIONS_PER_SERVICE,
                DependencyNamingPolicy.DEFAULT_HOSTNAME_DENYLIST_PATTERNS);
    }

    // Builds a processor whose emitted events are real JacksonEvents (so NodeOperationDetail fields
    // are queryable via event.get(...)), with a clock wired for the three-call window flush.
    private OTelApmServiceMapProcessor newFlushingProcessor(final String dirPrefix) {
        return newFlushingProcessor(dirPrefix, enabledDependencyNodes());
    }

    private OTelApmServiceMapProcessor newFlushingProcessor(final String dirPrefix,
                                                            final DependencyNodesConfig dependencyNodesConfig) {
        when(clock.instant())
                .thenReturn(testTime).thenReturn(testTime)
                .thenReturn(testTime.plusSeconds(65)).thenReturn(testTime.plusSeconds(65))
                .thenReturn(testTime.plusSeconds(65)).thenReturn(testTime.plusSeconds(65))
                .thenReturn(testTime.plusSeconds(130)).thenReturn(testTime.plusSeconds(130))
                .thenReturn(testTime.plusSeconds(130)).thenReturn(testTime.plusSeconds(130));
        return newProcessorCapturingEvents(dirPrefix, dependencyNodesConfig);
    }

    // Builds a processor whose emitted events are real JacksonEvents; the caller stubs the clock.
    private OTelApmServiceMapProcessor newProcessorCapturingEvents(final String dirPrefix) {
        return newProcessorCapturingEvents(dirPrefix, enabledDependencyNodes());
    }

    private OTelApmServiceMapProcessor newProcessorCapturingEvents(final String dirPrefix,
                                                                   final DependencyNodesConfig dependencyNodesConfig) {
        stubEventFactoryToBuildJacksonEvents();
        final File dir = new File(tempDir, dirPrefix + System.nanoTime());
        dir.mkdirs();
        return new OTelApmServiceMapProcessor(Duration.ofSeconds(60), dir, clock, 1, eventFactory, pluginMetrics,
                Collections.emptyList(), MetricTimestampSource.SPAN_END_TIME, MetricTimestampGranularity.SECONDS,
                dependencyNodesConfig);
    }

    private void stubEventFactoryToBuildJacksonEvents() {
        final BaseEventBuilder<Event> eventBuilder = mock(EventBuilder.class, RETURNS_DEEP_STUBS);
        when(eventFactory.eventBuilder(any())).thenReturn(eventBuilder);
        doAnswer(a -> {
            eventMetadata = a.getArgument(0);
            return eventBuilder;
        }).when(eventBuilder).withEventMetadata(any());
        doAnswer(a -> {
            eventData = a.getArgument(0);
            return eventBuilder;
        }).when(eventBuilder).withData(any());
        doAnswer(a -> JacksonEvent.builder().withEventMetadata(eventMetadata).withData(eventData).build())
                .when(eventBuilder).build();
    }

    // Runs the three-call flush and returns every emitted record (SERVICE_MAP events and metrics).
    private Collection<Record<Event>> flushAll(final OTelApmServiceMapProcessor proc,
                                               final List<Record<Event>> firstBatch) {
        proc.doExecute(firstBatch);
        proc.doExecute(Collections.emptyList());
        return proc.doExecute(Collections.emptyList());
    }

    private static List<Event> serviceMapEvents(final Collection<Record<Event>> records) {
        return records.stream()
                .filter(r -> r.getData().getMetadata() != null
                        && "SERVICE_MAP".equals(r.getData().getMetadata().getEventType()))
                .map(Record::getData)
                .collect(Collectors.toList());
    }

    // Runs the three-call flush and returns the emitted SERVICE_MAP node events.
    private List<Event> flushServiceMapEvents(final OTelApmServiceMapProcessor proc,
                                              final List<Record<Event>> firstBatch) {
        proc.doExecute(firstBatch);
        proc.doExecute(Collections.emptyList());
        final Collection<Record<Event>> result = proc.doExecute(Collections.emptyList());
        return result.stream()
                .filter(r -> r.getData().getMetadata() != null
                        && "SERVICE_MAP".equals(r.getData().getMetadata().getEventType()))
                .map(Record::getData)
                .collect(Collectors.toList());
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
