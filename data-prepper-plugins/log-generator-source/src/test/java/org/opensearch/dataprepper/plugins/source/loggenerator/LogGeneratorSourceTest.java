/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.loggenerator;


import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.loggenerator.LogGeneratorSourceConfig.INFINITE_LOG_COUNT;

@ExtendWith(MockitoExtension.class)
public class LogGeneratorSourceTest {
    @Mock
    private LogGeneratorSourceConfig sourceConfig;
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private PluginFactory pluginFactory;
    private LogGeneratorSource logGeneratorSource;
    @Mock
    private PluginModel mockLogPluginModel;
    @Mock
    private Buffer buffer;

    @BeforeEach
    public void setup() {
        when(sourceConfig.getLogType()).thenReturn(mockLogPluginModel);

        when(mockLogPluginModel.getPluginName()).thenReturn(
                "TestPluginName"
        );
        Map<String, Object> emptyMap = new HashMap<>();
        when(mockLogPluginModel.getPluginSettings()).thenReturn(
                emptyMap
        );
        LogTypeGenerator commonApacheLogTypeGenerator = mock(LogTypeGenerator.class);


        when(pluginFactory.loadPlugin(any(), any(PluginSetting.class))).thenReturn(commonApacheLogTypeGenerator);
        String sampleEventString = UUID.randomUUID().toString();
        Event stubbedJackson = JacksonEvent.fromMessage(sampleEventString);
        lenient().when(commonApacheLogTypeGenerator.generateEvent()).thenReturn(stubbedJackson); // JacksonEvent type
    }

    @AfterEach
    public void tearDown() { logGeneratorSource.stop(); }

    private LogGeneratorSource createObjectUnderTest() {
        return new LogGeneratorSource(sourceConfig, pluginMetrics, pluginFactory);
    }

    @Test
    void GIVEN_logGeneratorSourceAndBlockingBuffer_WHEN_noLimit_THEN_keepsWritingToBufferUntilStopped()
            throws TimeoutException {
        logGeneratorSource = createObjectUnderTest();

        Duration interval = Duration.ofMillis(100);

        lenient().when(sourceConfig.getInterval()).thenReturn(interval);
        lenient().when(sourceConfig.getCount()).thenReturn(INFINITE_LOG_COUNT); // no limit to log count

        logGeneratorSource.start(buffer);
        await()
                .atMost((long) (interval.toMillis() * 1.5), TimeUnit.MILLISECONDS)
                .untilAsserted(() -> verify(buffer, atLeast(1)).write(any(Record.class), anyInt()));
        verify(buffer, atLeast(1)).write(any(Record.class), anyInt());
        await()
                .atMost((long) (interval.toMillis() * 1.5), TimeUnit.MILLISECONDS)
                .untilAsserted(() -> verify(buffer, atLeast(1)).write(any(Record.class), anyInt()));
        verify(buffer, atLeast(2)).write(any(Record.class), anyInt());
    }

    @Test
    void GIVEN_logGeneratorSourceAndBlockingBuffer_WHEN_reachedLimit_THEN_stopsWritingToBuffer()
            throws InterruptedException, TimeoutException {
        logGeneratorSource = createObjectUnderTest();

        Duration interval = Duration.ofMillis(100);

        lenient().when(sourceConfig.getInterval()).thenReturn(interval);
        lenient().when(sourceConfig.getCount()).thenReturn(1); // max log count of 1 in logGeneratorSource

        verifyNoInteractions(buffer);

        logGeneratorSource.start(buffer);

        await()
                .atMost(interval.multipliedBy(3))
                .untilAsserted(() -> verify(buffer, atLeast(1)).write(any(Record.class), anyInt()));
        verify(buffer, times(1)).write(any(Record.class), anyInt());

        Thread.sleep((long) (interval.toMillis() * 1.1));
        verify(buffer, times(1)).write(any(Record.class), anyInt());
    }
}