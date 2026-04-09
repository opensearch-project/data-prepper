/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.http.service;

import org.opensearch.dataprepper.common.sink.SinkBufferEntry;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class HttpSinkBufferEntry implements SinkBufferEntry {
    private final Event event;
    private final long estimatedSize;

    public HttpSinkBufferEntry(final Event event, final OutputCodec codec, final OutputCodecContext codecContext) throws IOException {
        this.event = event;
        this.estimatedSize = calculateSize(event, codec, codecContext);
    }

    private long calculateSize(final Event event, final OutputCodec codec, final OutputCodecContext codecContext) throws IOException {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            codec.start(outputStream, event, codecContext);
            codec.writeEvent(event, outputStream);
            codec.complete(outputStream);
            return outputStream.size();
        }
    }

    @Override
    public long getEstimatedSize() {
        return estimatedSize;
    }

    @Override
    public boolean exceedsMaxEventSizeThreshold() {
        return false;
    }

    @Override
    public Event getEvent() {
        return event;
    }
}
