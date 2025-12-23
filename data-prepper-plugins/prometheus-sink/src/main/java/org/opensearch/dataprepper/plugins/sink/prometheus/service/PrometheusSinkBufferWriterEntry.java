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

import com.google.common.annotations.VisibleForTesting;
import software.amazon.awssdk.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class PrometheusSinkBufferWriterEntry {
    private static final Logger LOG = LoggerFactory.getLogger(PrometheusSinkBufferWriterEntry.class);
    private long lastSentTimestamp = -1L;
    private final int maxEntries;
    private final long windowMillis;
    private long seriesMaxTimestamp = -1L;
    private long realtimeSeriesMax;
    private final TreeMap<Long, PrometheusSinkBufferEntry> entries;

    public PrometheusSinkBufferWriterEntry(final long windowMillis, final int maxEntries) {
        this.maxEntries = maxEntries;
        this.windowMillis = windowMillis;
        this.entries = new TreeMap<>();
        this.realtimeSeriesMax = Instant.now().toEpochMilli();
    }
    
    public boolean add(final PrometheusSinkBufferEntry bufferEntry) {
        long time = bufferEntry.getTimeSeries().getTimestamp();
        if (time <= lastSentTimestamp) {
            return false; 
        }

        if (time > seriesMaxTimestamp) {
            seriesMaxTimestamp = time;
            realtimeSeriesMax = Instant.now().toEpochMilli();
        }

        entries.put(time, bufferEntry);
        if (entries.size() > maxEntries) {
            LOG.warn("Number of entries exceeded maxEntries");
            return false;
        }
        return true;
    }

    public long getSize() {
        return entries.size();
    }

    @VisibleForTesting
    long windowSize() {
        long timeOffset = Instant.now().toEpochMilli() - realtimeSeriesMax;
        return (seriesMaxTimestamp - windowMillis + timeOffset + 1);
    }

    public long getNumberOfEntriesReadyToFlush() {
        SortedMap<Long, PrometheusSinkBufferEntry> readyToFlush = entries.headMap(windowSize());
        return readyToFlush.size();
    }

    public long getSizeOfEntriesReadyToFlush() {
        return entries.headMap(windowSize())
              .values()
              .stream()
              .mapToLong(entry -> entry.getTimeSeries().getSize())
              .sum();
    }

    public Pair<List<PrometheusSinkBufferEntry>, Long> getEntriesReadyToFlush(final long maxEvents, final long maxSize) {
        if (entries.isEmpty()) {
            return Pair.of(Collections.emptyList(), 0L);
        }

        final long cutoff = windowSize();
        List<PrometheusSinkBufferEntry> toFlush = new ArrayList<>();

        SortedMap<Long, PrometheusSinkBufferEntry> readyToFlush = entries.headMap(cutoff);
        long numEntries = 0L;
        long curSize = 0L;
        if (!readyToFlush.isEmpty()) {
            Iterator<Map.Entry<Long, PrometheusSinkBufferEntry>> iterator = readyToFlush.entrySet().iterator();

            while (iterator.hasNext() && numEntries < maxEvents ) {
                Map.Entry<Long, PrometheusSinkBufferEntry> entry = iterator.next();
                long entrySize = entry.getValue().getTimeSeries().getSize();
    
                if (curSize + entrySize > maxSize) {
                    break;
                }
                curSize += entrySize;
                toFlush.add(entry.getValue());
                iterator.remove();
                numEntries++;
            }
        }
        
        if (!toFlush.isEmpty()) {
            toFlush.sort(Comparator.comparing(bufferEntry -> bufferEntry.getTimeSeries().getTimestamp()));
            this.lastSentTimestamp = toFlush.get(toFlush.size() - 1).getTimeSeries().getTimestamp();
        }
        return Pair.of(toFlush, curSize);
    }
}
