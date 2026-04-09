/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.http.service;

import org.opensearch.dataprepper.common.sink.DefaultSinkBuffer;
import org.opensearch.dataprepper.common.sink.SinkBufferEntry;
import org.opensearch.dataprepper.common.sink.SinkBufferWriter;

public class HttpSinkBuffer extends DefaultSinkBuffer {

    public HttpSinkBuffer(final long maxEvents, final long maxRequestSize, final long flushIntervalMs, final SinkBufferWriter sinkBufferWriter) {
        super(maxEvents, maxRequestSize, flushIntervalMs, sinkBufferWriter);
    }

    @Override
    public boolean isMaxEventsLimitReached() {
        return sinkBufferWriter.isMaxEventsLimitReached(maxEvents);
    }

    @Override
    public boolean willExceedMaxRequestSizeBytes(final SinkBufferEntry sinkBufferEntry) {
        return sinkBufferWriter.willExceedMaxRequestSizeBytes(sinkBufferEntry, maxRequestSize);
    }
}
