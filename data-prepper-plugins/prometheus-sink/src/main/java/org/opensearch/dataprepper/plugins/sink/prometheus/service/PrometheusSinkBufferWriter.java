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

import org.opensearch.dataprepper.common.sink.SinkBufferEntry;
import org.opensearch.dataprepper.common.sink.SinkBufferWriter;
import org.opensearch.dataprepper.common.sink.SinkFlushableBuffer;
import org.opensearch.dataprepper.common.sink.SinkFlushContext;
import org.opensearch.dataprepper.common.sink.SinkMetrics;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.PrometheusSinkConfiguration;
import software.amazon.awssdk.utils.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PrometheusSinkBufferWriter implements SinkBufferWriter {

    private static final int MAX_EVENTS_PER_SERIES = 1000;
    // Each buffer entry is a metric.
    // Duplicate entries for the same metric at the same time is not allowed
    // If there are multiple entries in the buffer for the same metric (with different time stamps),
    // They must be sorted by time before sending to Prometheus
    private final Map<String, PrometheusSinkBufferWriterEntry> buffer;
    private final SinkMetrics sinkMetrics;
    private final long outOfOrderWindowMillis;
    private final long maxEvents;
    private final long maxRequestSize;

    public PrometheusSinkBufferWriter(final PrometheusSinkConfiguration sinkConfig, final SinkMetrics sinkMetrics) {
        this.buffer = new HashMap<>();
        this.sinkMetrics = sinkMetrics;
        this.outOfOrderWindowMillis = sinkConfig.getOutOfOrderWindow().toMillis();
        this.maxEvents = sinkConfig.getThresholdConfig().getMaxEvents();
        this.maxRequestSize = sinkConfig.getThresholdConfig().getMaxRequestSizeBytes();
    }

    private String getSeriesKey(PrometheusSinkBufferEntry bufferEntry) {
        return bufferEntry.getTimeSeries().getMetricKey();
    }

    @Override
    public boolean isMaxEventsLimitReached(final long maxEvents) {
        return buffer.values().stream()
             .mapToLong(PrometheusSinkBufferWriterEntry::getNumberOfEntriesReadyToFlush)
             .sum() >= maxEvents;
    }

    @Override
    public boolean willExceedMaxRequestSizeBytes(final SinkBufferEntry sinkBufferEntry, final long maxRequestSize) {
        return buffer.values().stream()
             .mapToLong(PrometheusSinkBufferWriterEntry::getSizeOfEntriesReadyToFlush)
             .sum() >= maxRequestSize;
    }

    public boolean writeToBuffer(final SinkBufferEntry bufferEntry) {
        PrometheusSinkBufferEntry entry = (PrometheusSinkBufferEntry)bufferEntry;
        if (entry.getTimeSeries() == null || entry.getTimeSeries().getSize() >= maxRequestSize) {
            return false;
        }
        final String seriesKey = getSeriesKey(entry);

        boolean result = buffer.computeIfAbsent(seriesKey,
            k -> new PrometheusSinkBufferWriterEntry(this.outOfOrderWindowMillis, MAX_EVENTS_PER_SERIES)).add(entry);
        if (!result) {
            sinkMetrics.incrementEventsDroppedCounter(1);
        }
        return result;
    }

    @VisibleForTesting
    long getBufferSize() {
        return buffer.size();
    }

    @Override
    public SinkFlushableBuffer getBuffer(final SinkFlushContext sinkFlushContext) {
        List<SinkBufferEntry> bufferList = new ArrayList<>();
        long curMaxEvents = maxEvents;
        long curMaxSize = maxRequestSize;
        long reqSize = 0L;
        for (Map.Entry<String, PrometheusSinkBufferWriterEntry> entry : buffer.entrySet()) {
            if (curMaxEvents == 0 || curMaxSize == 0) {
                break;
            }
            Pair<List<PrometheusSinkBufferEntry>, Long> result = entry.getValue().getEntriesReadyToFlush(curMaxEvents, curMaxSize);
            if (!result.left().isEmpty()) {
                curMaxEvents -= result.left().size();
                curMaxSize -= result.right();
                reqSize += result.right();
                bufferList.addAll(result.left());
            }
        }
        if (bufferList.isEmpty()) {
            return null;
        }
        sinkMetrics.recordRequestSize(reqSize);
        buffer.entrySet().removeIf(entry -> entry.getValue().getSize() == 0);

        return new PrometheusSinkFlushableBuffer(bufferList, sinkMetrics, sinkFlushContext);
    }
}
