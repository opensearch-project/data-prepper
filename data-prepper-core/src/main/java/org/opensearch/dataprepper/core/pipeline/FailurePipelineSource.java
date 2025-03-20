/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.pipeline;

import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.model.failures.FailurePipeline;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

public class FailurePipelineSource implements Source<Record<Event>>, FailurePipeline {
    private static final Logger LOG = LoggerFactory.getLogger(FailurePipelineSource.class);
    private static final int DEFAULT_WRITE_TIMEOUT = Integer.MAX_VALUE;
    private Buffer buffer;
    private AtomicBoolean isStopRequested;

    public FailurePipelineSource() {
        isStopRequested = new AtomicBoolean(false);
    }

    @Override
    public void start(Buffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public void stop() {
        isStopRequested.set(true);
    }

    @Override
    public void sendFailedEvents(Collection<Record<Event>> records) {
        try {
            buffer.writeAll(records, DEFAULT_WRITE_TIMEOUT);
        } catch (Exception e) {
            LOG.error("Failed to write to failure pipeline");
        }
    }
 
}
