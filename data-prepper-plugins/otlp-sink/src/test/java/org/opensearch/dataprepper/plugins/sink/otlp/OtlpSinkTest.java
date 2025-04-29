/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.otlp;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.plugins.sink.otlp.buffer.OtlpSinkBuffer;
import org.opensearch.dataprepper.plugins.sink.otlp.configuration.OtlpSinkConfig;
import software.amazon.awssdk.regions.Region;

import java.lang.reflect.Field;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class OtlpSinkTest {
    private OtlpSink target;
    private OtlpSinkBuffer mockBuffer;
    private OtlpSinkConfig mockConfig;
    private PluginMetrics mockMetrics;
    private PluginSetting mockSetting;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("aws.region", Region.US_WEST_2.id());

        // Arrange: stub out config, metrics, setting
        mockConfig = mock(OtlpSinkConfig.class);
        mockMetrics = mock(PluginMetrics.class);
        mockSetting = mock(PluginSetting.class);
        when(mockSetting.getPipelineName()).thenReturn("pipeline");
        when(mockSetting.getName()).thenReturn("otlp");

        // Create the real sink
        target = new OtlpSink(mockConfig, mockMetrics, mockSetting);

        // Replace its private buffer with a mock
        mockBuffer = mock(OtlpSinkBuffer.class);
        final Field bufferField = OtlpSink.class.getDeclaredField("buffer");
        bufferField.setAccessible(true);
        bufferField.set(target, mockBuffer);
    }

    @AfterEach
    void tearDown() {
        System.clearProperty("aws.region");
    }

    @Test
    void testInitialize_startsBuffer() {
        // Act
        target.initialize();

        // Assert
        verify(mockBuffer).start();
    }

    @Test
    void testOutput_addsEveryRecordToBuffer() {
        // Arrange
        @SuppressWarnings("unchecked") final Record<Span> r1 = mock(Record.class);
        @SuppressWarnings("unchecked") final Record<Span> r2 = mock(Record.class);

        // Act
        target.output(List.of(r1, r2));

        // Assert
        verify(mockBuffer).add(r1);
        verify(mockBuffer).add(r2);
        verifyNoMoreInteractions(mockBuffer);
    }

    @Test
    void testIsReady_delegatesToBuffer() {
        // true case
        when(mockBuffer.isRunning()).thenReturn(true);
        assertTrue(target.isReady());

        // false case
        when(mockBuffer.isRunning()).thenReturn(false);
        assertFalse(target.isReady());
    }

    @Test
    void testShutdown_stopsBuffer() {
        // Act
        target.shutdown();

        // Assert
        verify(mockBuffer).stop();
    }

    @Test
    void testConstructor_doesNotThrow() {
        // Just ensure the three-arg constructor still works
        assertDoesNotThrow(() -> new OtlpSink(mockConfig, mockMetrics, mockSetting));
    }
}
