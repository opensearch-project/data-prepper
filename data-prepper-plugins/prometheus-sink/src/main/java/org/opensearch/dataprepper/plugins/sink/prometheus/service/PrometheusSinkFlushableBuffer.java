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

import org.opensearch.dataprepper.common.sink.SinkFlushableBuffer;
import org.opensearch.dataprepper.common.sink.SinkBufferEntry;
import org.opensearch.dataprepper.common.sink.SinkFlushResult;
import org.opensearch.dataprepper.common.sink.SinkFlushContext;
import org.opensearch.dataprepper.common.sink.SinkMetrics;
import org.opensearch.dataprepper.common.sink.DefaultSinkFlushResult;
import org.opensearch.dataprepper.plugins.sink.prometheus.PrometheusHttpSender;
import org.opensearch.dataprepper.plugins.sink.prometheus.PrometheusPushResult;
import org.opensearch.dataprepper.model.event.Event;

import com.arpnetworking.metrics.prometheus.Remote;
import com.arpnetworking.metrics.prometheus.Types;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class PrometheusSinkFlushableBuffer implements SinkFlushableBuffer {
    List<SinkBufferEntry> buffer;
    final PrometheusSinkFlushContext sinkFlushContext;
    final SinkMetrics sinkMetrics;

    public PrometheusSinkFlushableBuffer(List<SinkBufferEntry> buffer, final SinkMetrics sinkMetrics, final SinkFlushContext sinkFlushContext) {
        this.buffer = buffer;
        this.sinkMetrics = sinkMetrics;
        this.sinkFlushContext = (PrometheusSinkFlushContext)sinkFlushContext;
    }

    @Override
    public SinkFlushResult flush() {
        if (buffer.isEmpty()) {
            return null;
        }

        PrometheusHttpSender httpSender = sinkFlushContext.getHttpSender();
        final Remote.WriteRequest.Builder writeRequestBuilder = Remote.WriteRequest.newBuilder();
        List<Types.TimeSeries> allTimeSeries = new ArrayList<>(buffer.size() * 2);

        List<Event> events = new ArrayList<>(buffer.size());
        for (final SinkBufferEntry sinkBufferEntry : buffer) {
            PrometheusSinkBufferEntry bufferEntry = (PrometheusSinkBufferEntry)sinkBufferEntry;
            allTimeSeries.addAll(bufferEntry.getTimeSeries().getTimeSeriesList());
            events.add(bufferEntry.getEvent());
        }
        buffer.clear();
        writeRequestBuilder.addAllTimeseries(allTimeSeries);
        Remote.WriteRequest request = writeRequestBuilder.build();
        byte[] bytes = request.toByteArray();
        PrometheusPushResult result = httpSender.pushToEndpoint(bytes);
        if (!result.isSuccess()) {
            sinkMetrics.incrementRequestsFailedCounter(1);
            sinkMetrics.incrementEventsFailedCounter(events.size());
            return new DefaultSinkFlushResult(events, result.getStatusCode(), null);
        }
        sinkMetrics.incrementRequestsSuccessCounter(1);
        sinkMetrics.incrementEventsSuccessCounter(events.size());
        return null;
    }

    @Override
    public List<Event> getEvents() {
        return buffer.stream()
                .map(SinkBufferEntry::getEvent)
                .collect(Collectors.toList());
    }
}

