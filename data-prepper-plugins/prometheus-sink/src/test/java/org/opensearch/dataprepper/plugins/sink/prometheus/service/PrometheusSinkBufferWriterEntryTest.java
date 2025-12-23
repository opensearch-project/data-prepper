 /*
  * Copyright OpenSearch Contributors
  * SPDX-License-Identifier: Apache-2.0
  *
  * The OpenSearch Contributors require contributions made to
  * this file be licensed under the Apache-2.0 license or a
  * compatible open source license.
  *
  */

package org.opensearch.dataprepper.plugins.sink.prometheus.service;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import software.amazon.awssdk.utils.Pair;

import java.util.List;

public class PrometheusSinkBufferWriterEntryTest {
    static final long TEST_WINDOW_MS = 2000;
    static final int TEST_MAX_ENTRIES = 100;
    static final int TEST_NUM_ENTRIES = 10;

    private PrometheusSinkBufferWriterEntry sinkBufferWriterEntry;
    private long timeStamp;
    

    private PrometheusSinkBufferWriterEntry createObjectUnderTest() {
        return new PrometheusSinkBufferWriterEntry(TEST_WINDOW_MS, TEST_MAX_ENTRIES);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 5, 8, 10})
    public void testBufferEntryWriterMaxEvents(final int maxEvents) {
        sinkBufferWriterEntry = createObjectUnderTest();
        assertThat(sinkBufferWriterEntry.getNumberOfEntriesReadyToFlush(), equalTo(0L));
        assertThat(sinkBufferWriterEntry.getSizeOfEntriesReadyToFlush(), equalTo(0L));
        Pair<List<PrometheusSinkBufferEntry>, Long> result = sinkBufferWriterEntry.getEntriesReadyToFlush(1, 100L);
        assertTrue(result.left().isEmpty());
        assertThat(result.right(), equalTo(0L));
        for (int i = 0; i < TEST_NUM_ENTRIES; i++) {
            long timeStamp = (i+1)*1000L;
            PrometheusTimeSeries timeSeries = mock(PrometheusTimeSeries.class);
            when(timeSeries.getTimestamp()).thenReturn(timeStamp);
            when(timeSeries.getSize()).thenReturn(100);
            PrometheusSinkBufferEntry bufferEntry = mock(PrometheusSinkBufferEntry.class);
            when(bufferEntry.getTimeSeries()).thenReturn(timeSeries);
            assertThat(sinkBufferWriterEntry.add(bufferEntry), equalTo(true));
        }
        assertThat(sinkBufferWriterEntry.getSize(), equalTo(10L));

        try {
            Thread.sleep(3000);
        } catch (Exception e) {}

        int remainingEvents = TEST_NUM_ENTRIES;
        while (remainingEvents > 0) {
            int expectedEvents = Math.min(maxEvents, remainingEvents);
            result = sinkBufferWriterEntry.getEntriesReadyToFlush(maxEvents, 2000L);
            assertThat(result.left().size(), equalTo(expectedEvents));
            remainingEvents -= result.left().size();
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {100L, 200L, 500L, 800L, 1000L})
    public void testBufferEntryWriterMaxSize(final long maxSize) {
        sinkBufferWriterEntry = createObjectUnderTest();
        assertThat(sinkBufferWriterEntry.getNumberOfEntriesReadyToFlush(), equalTo(0L));
        assertThat(sinkBufferWriterEntry.getSizeOfEntriesReadyToFlush(), equalTo(0L));
        Pair<List<PrometheusSinkBufferEntry>, Long> result = sinkBufferWriterEntry.getEntriesReadyToFlush(1, 100L);
        assertTrue(result.left().isEmpty());
        assertThat(result.right(), equalTo(0L));
        for (int i = 0; i < TEST_NUM_ENTRIES; i++) {
            long timeStamp = (i+1)*1000L;
            PrometheusTimeSeries timeSeries = mock(PrometheusTimeSeries.class);
            when(timeSeries.getTimestamp()).thenReturn(timeStamp);
            when(timeSeries.getSize()).thenReturn(100);
            PrometheusSinkBufferEntry bufferEntry = mock(PrometheusSinkBufferEntry.class);
            when(bufferEntry.getTimeSeries()).thenReturn(timeSeries);
            assertThat(sinkBufferWriterEntry.add(bufferEntry), equalTo(true));
        }
        assertThat(sinkBufferWriterEntry.getSize(), equalTo(10L));

        try {
            Thread.sleep(3000);
        } catch (Exception e) {}

        int remainingEvents = TEST_NUM_ENTRIES;
        while (remainingEvents > 0) {
            long expectedSize = Math.min(maxSize, remainingEvents * 100L);
            result = sinkBufferWriterEntry.getEntriesReadyToFlush(20, maxSize);
            assertThat(result.right(), equalTo(expectedSize));
            remainingEvents -= result.left().size();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 5, 8, 10})
    public void testAddingOldEventsGetRejected(final int numEvents) {
        long timeStamp = 0;
        sinkBufferWriterEntry = createObjectUnderTest();
        for (int i = 0; i < numEvents; i++) {
            timeStamp = (i+1)*1000L;
            PrometheusTimeSeries timeSeries = mock(PrometheusTimeSeries.class);
            when(timeSeries.getTimestamp()).thenReturn(timeStamp);
            when(timeSeries.getSize()).thenReturn(100);
            PrometheusSinkBufferEntry bufferEntry = mock(PrometheusSinkBufferEntry.class);
            when(bufferEntry.getTimeSeries()).thenReturn(timeSeries);
            assertThat(sinkBufferWriterEntry.add(bufferEntry), equalTo(true));
        }
        try {
            Thread.sleep(3000);
        } catch (Exception e) {}
        Pair<List<PrometheusSinkBufferEntry>, Long> result = sinkBufferWriterEntry.getEntriesReadyToFlush(20, 2000L);
        assertThat(result.left().size(), equalTo(numEvents));
        PrometheusTimeSeries timeSeries = mock(PrometheusTimeSeries.class);
        when(timeSeries.getTimestamp()).thenReturn(timeStamp - 100L);
        when(timeSeries.getSize()).thenReturn(100);
        PrometheusSinkBufferEntry bufferEntry = mock(PrometheusSinkBufferEntry.class);
        when(bufferEntry.getTimeSeries()).thenReturn(timeSeries);
        assertThat(sinkBufferWriterEntry.add(bufferEntry), equalTo(false));
        assertThat(sinkBufferWriterEntry.getSize(), equalTo(0L));
    }
}
