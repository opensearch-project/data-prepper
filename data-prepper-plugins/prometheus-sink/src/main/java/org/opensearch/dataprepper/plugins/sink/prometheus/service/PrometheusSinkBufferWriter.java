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

import org.opensearch.dataprepper.common.sink.SinkBufferEntry;
import org.opensearch.dataprepper.common.sink.SinkBufferWriter;
import org.opensearch.dataprepper.common.sink.SinkFlushableBuffer;
import org.opensearch.dataprepper.common.sink.SinkFlushContext;
import org.opensearch.dataprepper.common.sink.SinkMetrics;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PrometheusSinkBufferWriter implements SinkBufferWriter {

    // Each buffer entry is a metric.
    // Duplicate entries for the same metric at the same time is not allowed
    // If there are multiple entries in the buffer for the same metric (with different time stamps),
    // They must be sorted by time before sending to Prometheus
    private final Map<String, Map<Long, SinkBufferEntry>> buffer;
    private final SinkMetrics sinkMetrics;

    public PrometheusSinkBufferWriter(SinkMetrics sinkMetrics) {
        this.buffer = new HashMap<>();
        this.sinkMetrics = sinkMetrics;
    }

    public boolean writeToBuffer(SinkBufferEntry bufferEntry) {
        PrometheusTimeSeries timeSeries = ((PrometheusSinkBufferEntry)bufferEntry).getTimeSeries();
        if (timeSeries == null) {
            return false;
        }

        buffer.computeIfAbsent(timeSeries.getMetricName(), k -> new HashMap<>())
              .put(timeSeries.getTimeStamp(), bufferEntry);
        return true;
    }

    @Override
    public SinkFlushableBuffer getBuffer(final SinkFlushContext sinkFlushContext) {
        List<SinkBufferEntry> bufferList = buffer.values().stream()
            .flatMap(timeSeriesMap -> timeSeriesMap.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(Map.Entry::getValue))
            .collect(Collectors.toList());

        buffer.clear();
        return new PrometheusSinkFlushableBuffer(bufferList, sinkMetrics, sinkFlushContext);
    }
}
