/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.prometheus.service;

import org.opensearch.dataprepper.common.sink.SinkFlushableBuffer;
import org.opensearch.dataprepper.common.sink.SinkBufferEntry;
import org.opensearch.dataprepper.common.sink.SinkFlushResult;
import org.opensearch.dataprepper.common.sink.SinkFlushContext;
import org.opensearch.dataprepper.common.sink.SinkMetrics;
import org.opensearch.dataprepper.common.sink.DefaultSinkFlushResult;
import org.opensearch.dataprepper.plugins.sink.prometheus.PrometheusHttpSender;
import org.opensearch.dataprepper.model.event.Event;

import com.arpnetworking.metrics.prometheus.Remote;
import com.arpnetworking.metrics.prometheus.Types;

import software.amazon.awssdk.utils.Pair;

import java.util.ArrayList;
import java.util.List;

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
        if (buffer.size() == 0) {
            return null;
        }

        PrometheusHttpSender httpSender = sinkFlushContext.getHttpSender();
        final Remote.WriteRequest.Builder writeRequestBuilder = Remote.WriteRequest.newBuilder();
        List<Types.TimeSeries> allTimeSeries = new ArrayList<>();

        List<Event> events = new ArrayList<>();
        for (final SinkBufferEntry sinkBufferEntry : buffer) {
            PrometheusSinkBufferEntry bufferEntry = (PrometheusSinkBufferEntry)sinkBufferEntry;
            allTimeSeries.addAll(bufferEntry.getTimeSeries().getTimeSeriesList());
            events.add(bufferEntry.getEvent());
        }
        buffer.clear();
        writeRequestBuilder.addAllTimeseries(allTimeSeries);
        Remote.WriteRequest request = writeRequestBuilder.build();
        byte[] bytes = request.toByteArray();
        Pair<Boolean, Integer> result = httpSender.pushToEndPoint(bytes);
        if (!result.left()) {
            sinkMetrics.incrementRequestsFailedCounter(1);
            sinkMetrics.incrementEventsFailedCounter(events.size());
            return new DefaultSinkFlushResult(events, result.right(), null);
        }
        sinkMetrics.incrementRequestsSuccessCounter(1);
        sinkMetrics.incrementEventsSuccessCounter(events.size());
        return null;
    }

    @Override
    public List<Event> getEvents() {
        List<Event> result = new ArrayList<>();
        for (final SinkBufferEntry bufferEntry: buffer) {
            result.add(bufferEntry.getEvent());
        }
        return result;
    }
}

