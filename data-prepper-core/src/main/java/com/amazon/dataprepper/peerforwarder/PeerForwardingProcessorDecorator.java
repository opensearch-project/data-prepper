/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.peerforwarder;

import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.processor.Processor;
import com.amazon.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class PeerForwardingProcessorDecorator implements Processor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(PeerForwardingProcessorDecorator.class);

    private final Processor innerProcessor;

    public PeerForwardingProcessorDecorator(final Processor innerProcessor) {
        this.innerProcessor = innerProcessor;
        LOG.info("Peer Forwarder not implemented yet, processing events locally.");
    }

    @Override
    public Collection<Record<Event>> execute(final Collection<Record<Event>> records) {
        return innerProcessor.execute(records);
    }

    @Override
    public void prepareForShutdown() {
        innerProcessor.prepareForShutdown();
    }

    @Override
    public boolean isReadyForShutdown() {
        return innerProcessor.isReadyForShutdown();
    }

    @Override
    public void shutdown() {
        innerProcessor.shutdown();
    }
}
