/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.http.service;

import org.opensearch.dataprepper.common.sink.DefaultSinkFlushResult;
import org.opensearch.dataprepper.common.sink.SinkBufferEntry;
import org.opensearch.dataprepper.common.sink.SinkFlushContext;
import org.opensearch.dataprepper.common.sink.SinkFlushResult;
import org.opensearch.dataprepper.common.sink.SinkFlushableBuffer;
import org.opensearch.dataprepper.common.sink.SinkMetrics;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.plugins.sink.http.HttpEndpointResponse;
import org.opensearch.dataprepper.plugins.sink.http.HttpSinkSender;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class HttpSinkFlushableBuffer implements SinkFlushableBuffer {
    private final List<SinkBufferEntry> buffer;
    private final HttpSinkFlushContext flushContext;
    private final SinkMetrics sinkMetrics;

    public HttpSinkFlushableBuffer(final List<SinkBufferEntry> buffer, final SinkMetrics sinkMetrics, final SinkFlushContext flushContext) {
        this.buffer = buffer;
        this.sinkMetrics = sinkMetrics;
        this.flushContext = (HttpSinkFlushContext) flushContext;
    }

    @Override
    public SinkFlushResult flush() {
        if (buffer.isEmpty()) {
            return null;
        }

        final HttpSinkSender httpSender = flushContext.getHttpSender();
        final OutputCodec codec = flushContext.getCodec();
        final OutputCodecContext codecContext = flushContext.getCodecContext();
        final List<Event> events = getEvents();

        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            final OutputCodec.Writer writer = codec.createWriter(outputStream, buffer.get(0).getEvent(), codecContext);
            for (final SinkBufferEntry entry : buffer) {
                writer.writeEvent(entry.getEvent());
            }
            writer.complete();

            final byte[] data = outputStream.toByteArray();
            final HttpEndpointResponse response = httpSender.send(data);

            if (response.getStatusCode() >= 200 && response.getStatusCode() < 300) {
                sinkMetrics.incrementRequestsSuccessCounter(1);
                sinkMetrics.incrementEventsSuccessCounter(events.size());
                return null;
            } else {
                sinkMetrics.incrementRequestsFailedCounter(1);
                sinkMetrics.incrementEventsFailedCounter(events.size());
                return new DefaultSinkFlushResult(events, response.getStatusCode(), new RuntimeException(response.getErrMessage()));
            }
        } catch (IOException e) {
            sinkMetrics.incrementRequestsFailedCounter(1);
            sinkMetrics.incrementEventsFailedCounter(events.size());
            return new DefaultSinkFlushResult(events, 0, e);
        }
    }

    @Override
    public List<Event> getEvents() {
        return buffer.stream()
                .map(SinkBufferEntry::getEvent)
                .collect(Collectors.toList());
    }
}
