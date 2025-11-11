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

import java.util.ArrayList;
import java.util.List;

public class PrometheusSinkBufferWriter implements SinkBufferWriter {
    
    private final List<SinkBufferEntry> buffer;
    private final SinkMetrics sinkMetrics;

    public PrometheusSinkBufferWriter(SinkMetrics sinkMetrics) {
        buffer = new ArrayList<>();
        this.sinkMetrics = sinkMetrics;
    }

    public boolean writeToBuffer(SinkBufferEntry bufferEntry) {
        buffer.add(bufferEntry);
        return true;
    }

    public SinkFlushableBuffer getBuffer(final SinkFlushContext sinkFlushContext) {
        return new PrometheusSinkFlushableBuffer(buffer, sinkMetrics, sinkFlushContext);
    }

}

    

