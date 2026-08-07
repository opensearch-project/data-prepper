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

import org.opensearch.dataprepper.common.sink.DefaultSinkBuffer;
import org.opensearch.dataprepper.common.sink.SinkBufferWriter;
import org.opensearch.dataprepper.common.sink.SinkBufferEntry;

public class PrometheusSinkBuffer extends DefaultSinkBuffer {

    public PrometheusSinkBuffer(final long maxEvents, final long maxRequestSize, 
        final long flushIntervalMs, final SinkBufferWriter sinkBufferWriter) {

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
