/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.file;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FileTailMetricsTest {

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Counter linesReadCounter;

    @Mock
    private Counter bytesReadCounter;

    @Mock
    private Counter linesTruncatedCounter;

    @Mock
    private Counter filesOpenedCounter;

    @Mock
    private Counter filesClosedCounter;

    @Mock
    private Counter filesRotatedCounter;

    @Mock
    private Counter readErrorsCounter;

    @Mock
    private Counter writeTimeoutsCounter;

    @Mock
    private Counter checkpointFlushesCounter;

    @Mock
    private Counter checkpointErrorsCounter;

    @Mock
    private Counter eventsEmittedCounter;

    @Mock
    private Counter dataLossEventsCounter;

    @Mock
    private Counter acknowledgmentFailuresCounter;

    @Mock
    private Timer backpressureTimer;

    private FileTailMetrics fileTailMetrics;

    @BeforeEach
    void setUp() {
        when(pluginMetrics.counter("tailLinesRead")).thenReturn(linesReadCounter);
        when(pluginMetrics.counter("tailBytesRead")).thenReturn(bytesReadCounter);
        when(pluginMetrics.counter("tailLinesTruncated")).thenReturn(linesTruncatedCounter);
        when(pluginMetrics.counter("tailFilesOpened")).thenReturn(filesOpenedCounter);
        when(pluginMetrics.counter("tailFilesClosed")).thenReturn(filesClosedCounter);
        when(pluginMetrics.counter("tailFilesRotated")).thenReturn(filesRotatedCounter);
        when(pluginMetrics.counter("tailReadErrors")).thenReturn(readErrorsCounter);
        when(pluginMetrics.counter("tailWriteTimeouts")).thenReturn(writeTimeoutsCounter);
        when(pluginMetrics.counter("tailCheckpointFlushes")).thenReturn(checkpointFlushesCounter);
        when(pluginMetrics.counter("tailCheckpointErrors")).thenReturn(checkpointErrorsCounter);
        when(pluginMetrics.counter("tailEventsEmitted")).thenReturn(eventsEmittedCounter);
        when(pluginMetrics.counter("tailDataLossEvents")).thenReturn(dataLossEventsCounter);
        when(pluginMetrics.counter("tailAcknowledgmentFailures")).thenReturn(acknowledgmentFailuresCounter);
        when(pluginMetrics.timer("tailBackpressureTime")).thenReturn(backpressureTimer);

        fileTailMetrics = new FileTailMetrics(pluginMetrics);
    }

    @Test
    void getLinesReadReturnsRegisteredCounter() {
        assertThat(fileTailMetrics.getLinesRead(), equalTo(linesReadCounter));
    }

    @Test
    void getBytesReadReturnsRegisteredCounter() {
        assertThat(fileTailMetrics.getBytesRead(), equalTo(bytesReadCounter));
    }

    @Test
    void getLinesTruncatedReturnsRegisteredCounter() {
        assertThat(fileTailMetrics.getLinesTruncated(), equalTo(linesTruncatedCounter));
    }

    @Test
    void getFilesOpenedReturnsRegisteredCounter() {
        assertThat(fileTailMetrics.getFilesOpened(), equalTo(filesOpenedCounter));
    }

    @Test
    void getFilesClosedReturnsRegisteredCounter() {
        assertThat(fileTailMetrics.getFilesClosed(), equalTo(filesClosedCounter));
    }

    @Test
    void getFilesRotatedReturnsRegisteredCounter() {
        assertThat(fileTailMetrics.getFilesRotated(), equalTo(filesRotatedCounter));
    }

    @Test
    void getReadErrorsReturnsRegisteredCounter() {
        assertThat(fileTailMetrics.getReadErrors(), equalTo(readErrorsCounter));
    }

    @Test
    void getWriteTimeoutsReturnsRegisteredCounter() {
        assertThat(fileTailMetrics.getWriteTimeouts(), equalTo(writeTimeoutsCounter));
    }

    @Test
    void getCheckpointFlushesReturnsRegisteredCounter() {
        assertThat(fileTailMetrics.getCheckpointFlushes(), equalTo(checkpointFlushesCounter));
    }

    @Test
    void getCheckpointErrorsReturnsRegisteredCounter() {
        assertThat(fileTailMetrics.getCheckpointErrors(), equalTo(checkpointErrorsCounter));
    }

    @Test
    void getActiveFileCountReturnsAtomicLong() {
        assertThat(fileTailMetrics.getActiveFileCount(), notNullValue());
        assertThat(fileTailMetrics.getActiveFileCount(), instanceOf(AtomicLong.class));
    }

    @Test
    void activeFileCountInitializesToZero() {
        assertThat(fileTailMetrics.getActiveFileCount().get(), equalTo(0L));
    }

    @Test
    void activeFileCountCanBeIncremented() {
        fileTailMetrics.getActiveFileCount().incrementAndGet();

        assertThat(fileTailMetrics.getActiveFileCount().get(), equalTo(1L));
    }

    @Test
    void activeFileCountCanBeDecrementedAfterIncrement() {
        fileTailMetrics.getActiveFileCount().incrementAndGet();
        fileTailMetrics.getActiveFileCount().incrementAndGet();
        fileTailMetrics.getActiveFileCount().decrementAndGet();

        assertThat(fileTailMetrics.getActiveFileCount().get(), equalTo(1L));
    }

    @Test
    void allCounterGettersReturnInstanceOfCounter() {
        assertThat(fileTailMetrics.getLinesRead(), instanceOf(Counter.class));
        assertThat(fileTailMetrics.getBytesRead(), instanceOf(Counter.class));
        assertThat(fileTailMetrics.getLinesTruncated(), instanceOf(Counter.class));
        assertThat(fileTailMetrics.getFilesOpened(), instanceOf(Counter.class));
        assertThat(fileTailMetrics.getFilesClosed(), instanceOf(Counter.class));
        assertThat(fileTailMetrics.getFilesRotated(), instanceOf(Counter.class));
        assertThat(fileTailMetrics.getReadErrors(), instanceOf(Counter.class));
        assertThat(fileTailMetrics.getWriteTimeouts(), instanceOf(Counter.class));
        assertThat(fileTailMetrics.getCheckpointFlushes(), instanceOf(Counter.class));
        assertThat(fileTailMetrics.getCheckpointErrors(), instanceOf(Counter.class));
    }

    @Test
    void getEventsEmittedReturnsRegisteredCounter() {
        assertThat(fileTailMetrics.getEventsEmitted(), equalTo(eventsEmittedCounter));
    }

    @Test
    void getBackpressureTimerReturnsRegisteredTimer() {
        assertThat(fileTailMetrics.getBackpressureTimer(), equalTo(backpressureTimer));
    }

    @Test
    void getFileLagBytesReturnsAtomicLong() {
        assertThat(fileTailMetrics.getFileLagBytes(), notNullValue());
        assertThat(fileTailMetrics.getFileLagBytes(), instanceOf(AtomicLong.class));
    }

    @Test
    void fileLagBytesInitializesToZero() {
        assertThat(fileTailMetrics.getFileLagBytes().get(), equalTo(0L));
    }

    @Test
    void getDataLossEventsReturnsRegisteredCounter() {
        assertThat(fileTailMetrics.getDataLossEvents(), equalTo(dataLossEventsCounter));
    }

    @Test
    void getAcknowledgmentFailuresReturnsRegisteredCounter() {
        assertThat(fileTailMetrics.getAcknowledgmentFailures(), equalTo(acknowledgmentFailuresCounter));
    }
}
