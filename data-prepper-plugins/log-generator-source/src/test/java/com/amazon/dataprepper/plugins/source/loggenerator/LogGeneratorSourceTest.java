/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.loggenerator;


import com.amazon.dataprepper.model.event.JacksonEvent;
import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.configuration.PluginModel;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.plugin.PluginFactory;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;

import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.TimeoutException;
import java.time.Duration;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.lenient;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.Assert.assertEquals;

public class LogGeneratorSourceTest {
    private LogGeneratorSourceConfig sourceConfig;
    private PluginMetrics pluginMetrics;
    private PluginFactory pluginFactory;
    private LogGeneratorSource logGeneratorSource;

    @BeforeEach
    public void setup() {
        sourceConfig = mock(LogGeneratorSourceConfig.class);
        pluginMetrics = mock(PluginMetrics.class);
        pluginFactory = mock(PluginFactory.class);

        PluginModel mockLogPluginModel = mock(PluginModel.class);
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
        String hardcodedEvent = "127.0.0.1 user pwd [date] \"GET /list HTTP/1.1\" 404 4043";
        Event stubbedJackson = JacksonEvent.fromMessage(hardcodedEvent);
        lenient().when(commonApacheLogTypeGenerator.generateEvent()).thenReturn(stubbedJackson); // JacksonEvent type

        logGeneratorSource = new LogGeneratorSource(sourceConfig, pluginMetrics, pluginFactory);
    }

    @Test
    void GIVEN_logGeneratorSourceAndBlockingBuffer_WHEN_noLimit_THEN_keepsWritingToBufferUntilStopped()
            throws InterruptedException, TimeoutException {
        BlockingBuffer<Record<Event>> spyBuffer = spy(new BlockingBuffer<Record<Event>>("SamplePipeline"));

        lenient().when(sourceConfig.getInterval()).thenReturn(Duration.ofSeconds(1)); // interval of 1 second
        lenient().when(sourceConfig.getCount()).thenReturn(0); // no limit to log count

        logGeneratorSource.start(spyBuffer);
        Thread.sleep(1500);

        verify(spyBuffer, atLeast(1)).write(any(Record.class), anyInt());
        Thread.sleep(700);
        verify(spyBuffer, atLeast(2)).write(any(Record.class), anyInt());
        logGeneratorSource.stop();
    }

    @Test
    void GIVEN_logGeneratorSourceAndBlockingBuffer_WHEN_reachedLimit_THEN_stopsWritingToBuffer()
            throws InterruptedException, TimeoutException {
        BlockingBuffer<Record<Event>> spyBuffer = spy(new BlockingBuffer<Record<Event>>("SamplePipeline"));

        lenient().when(sourceConfig.getInterval()).thenReturn(Duration.ofSeconds(1)); // interval of 1 second
        lenient().when(sourceConfig.getCount()).thenReturn(1); // max log count of 1 in logGeneratorSource

        logGeneratorSource.start(spyBuffer);
        assertEquals(spyBuffer.isEmpty(), true);
        Thread.sleep(1100);

        verify(spyBuffer, times(1)).write(any(Record.class), anyInt());

        Thread.sleep(1000);
        verify(spyBuffer, times(1)).write(any(Record.class), anyInt());
        logGeneratorSource.stop();
    }

    // Below test is possibly redundant because of test:
    // GIVEN_logGeneratorSourceAndBlockingBuffer_WHEN_noLimit_THEN_keepsWritingToBufferUntilStopped
//    @Test
//    void GIVEN_logGeneratorSourceAndBlockingBuffer_WHEN_stopCalled_THEN_stopsWritingToBuffer()
//            throws InterruptedException, TimeoutException {
//        BlockingBuffer<Record<Event>> spyBuffer = spy(new BlockingBuffer<Record<Event>>("SamplePipeline"));
//
//        lenient().when(sourceConfig.getInterval()).thenReturn(Duration.ofSeconds(1)); // interval of 1 second
//        lenient().when(sourceConfig.getCount()).thenReturn(2); // max count of 2
//
//        logGeneratorSource.start(spyBuffer);
//        assertEquals(spyBuffer.isEmpty(), true);
//        Thread.sleep(2500);
//
//        verify(spyBuffer, times(2)).write(any(Record.class), anyInt());
//        logGeneratorSource.stop();
//        Thread.sleep(500);
//        verify(spyBuffer, times(2)).write(any(Record.class), anyInt());
//    }
    // this test is also possibly redundant
//    @Test
//    void GIVEN_logGeneratorSourceAndBlockingBuffer_WHEN_startCalled_THEN_bufferIsNotEmpty()
//            throws InterruptedException, TimeoutException {
//        // also need to mock the buffer class
//        // arbitrary buffer settings
////        try {
//        // BlockingBuffer as the Buffer here was arbitrary
//        BlockingBuffer<Record<Event>> spyBuffer = spy(new BlockingBuffer<Record<Event>>("SamplePipeline"));
//
//        lenient().when(sourceConfig.getInterval()).thenReturn(Duration.ofSeconds(1)); // interval of 1 second
//        lenient().when(sourceConfig.getCount()).thenReturn(2); // max count of 2
//
//        logGeneratorSource.start(spyBuffer);
//        assertEquals(spyBuffer.isEmpty(), true);
//        Thread.sleep(3000);
//        assertEquals(spyBuffer.isEmpty(), false);
//        System.out.println("Succeeded beyond the Thread sleep");
//
//        logGeneratorSource.stop();
//    }
}