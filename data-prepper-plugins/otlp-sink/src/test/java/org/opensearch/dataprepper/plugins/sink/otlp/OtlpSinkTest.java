/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.otlp;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
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
import static org.mockito.Mockito.doThrow;
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
    private AwsCredentialsSupplier mockAwsCredSupplier;

    @BeforeEach
    void setUp() throws Exception {
        // Arrange: stub out config, metrics, setting
        mockAwsCredSupplier = mock(AwsCredentialsSupplier.class);
        mockConfig = mock(OtlpSinkConfig.class);
        when(mockConfig.getAwsRegion()).thenReturn(Region.of("us-west-2"));
        when(mockConfig.getEndpoint()).thenReturn("https://localhost/v1/traces");

        mockMetrics = mock(PluginMetrics.class);

        mockSetting = mock(PluginSetting.class);
        when(mockSetting.getPipelineName()).thenReturn("pipeline");
        when(mockSetting.getName()).thenReturn("otlp");

        // Create the real sink
        target = new OtlpSink(mockAwsCredSupplier, mockConfig, mockMetrics, mockSetting);

        // Replace its private buffer with a mock
        mockBuffer = mock(OtlpSinkBuffer.class);
        final Field bufferField = OtlpSink.class.getDeclaredField("buffer");
        bufferField.setAccessible(true);
        bufferField.set(target, mockBuffer);
    }

    @Test
    void testInitialize_startsBuffer() {
        // Act
        target.initialize();

        // Assert
        verify(mockBuffer).start();
    }

    @Test
    void testConstructor_throwsWhenAwsConfigIsMissing() {
        doThrow(new IllegalArgumentException("aws configuration is required"))
                .when(mockConfig).validate();

        // Act & Assert
        final Executable constructorCall = () ->
                new OtlpSink(mockAwsCredSupplier, mockConfig, mockMetrics, mockSetting);

        final IllegalArgumentException thrown = Assertions.assertThrows(
                IllegalArgumentException.class,
                constructorCall
        );

        Assertions.assertEquals("aws configuration is required", thrown.getMessage());
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
    void testIsReady_returnsTrueOnlyAfterInitialization() {
        when(mockBuffer.isRunning()).thenReturn(true);

        // Not initialized yet
        assertFalse(target.isReady());

        // Initialize, which sets 'initialized = true' and starts the buffer
        target.initialize();
        assertTrue(target.isReady());

        // Now simulate buffer being not running
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
        assertDoesNotThrow(() -> new OtlpSink(mockAwsCredSupplier, mockConfig, mockMetrics, mockSetting));
    }
}
