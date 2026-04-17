/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.otlp;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
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
    private AwsCredentialsSupplier mockAwsCredSupplier;

    @BeforeEach
    void setUp() throws Exception {
        mockAwsCredSupplier = mock(AwsCredentialsSupplier.class);
        mockConfig = mock(OtlpSinkConfig.class);
        when(mockConfig.getAwsRegion()).thenReturn(Region.of("us-west-2"));
        when(mockConfig.getEndpoint()).thenReturn("https://localhost/v1/traces");

        mockMetrics = mock(PluginMetrics.class);

        mockSetting = mock(PluginSetting.class);
        when(mockSetting.getPipelineName()).thenReturn("pipeline");
        when(mockSetting.getName()).thenReturn("otlp");

        target = new OtlpSink(mockAwsCredSupplier, mockConfig, mockMetrics, mockSetting);

        mockBuffer = mock(OtlpSinkBuffer.class);
        final Field bufferField = OtlpSink.class.getDeclaredField("buffer");
        bufferField.setAccessible(true);
        bufferField.set(target, mockBuffer);
    }

    @Test
    void testInitialize_startsBuffer() {
        target.initialize();
        verify(mockBuffer).start();
    }

    @Test
    void testOutput_addsEveryRecordToBuffer() {
        @SuppressWarnings("unchecked") final Record<Event> r1 = mock(Record.class);
        @SuppressWarnings("unchecked") final Record<Event> r2 = mock(Record.class);

        target.output(List.of(r1, r2));

        verify(mockBuffer).add(r1);
        verify(mockBuffer).add(r2);
        verifyNoMoreInteractions(mockBuffer);
    }

    @Test
    void testIsReady_returnsTrueOnlyAfterInitialization() {
        when(mockBuffer.isRunning()).thenReturn(true);

        assertFalse(target.isReady());

        target.initialize();
        assertTrue(target.isReady());

        when(mockBuffer.isRunning()).thenReturn(false);
        assertFalse(target.isReady());
    }

    @Test
    void testShutdown_stopsBuffer() {
        target.shutdown();
        verify(mockBuffer).stop();
    }

    @Test
    void testConstructor_doesNotThrow() {
        assertDoesNotThrow(() -> new OtlpSink(mockAwsCredSupplier, mockConfig, mockMetrics, mockSetting));
    }
}
