/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder;

import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.peerforwarder.RequiresPeerForwarding;
import com.amazon.dataprepper.model.processor.Processor;
import com.amazon.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public class PeerForwardingProcessorDecorator implements Processor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(PeerForwardingProcessorDecorator.class);

    private final Processor innerProcessor;
    private final PeerForwarder peerForwarder;

    public PeerForwardingProcessorDecorator(final Processor innerProcessor,
                                            final PeerForwarder peerForwarder
                                            ) {
        this.innerProcessor = innerProcessor;
        this.peerForwarder = peerForwarder;
        LOG.info("Peer Forwarder not implemented yet, processing events locally.");
    }

    @Override
    public Collection<Record<Event>> execute(final Collection<Record<Event>> records) {
        Set<String> identificationKeys = null;

        if (innerProcessor instanceof RequiresPeerForwarding) {
            identificationKeys = ((RequiresPeerForwarding) innerProcessor).getIdentificationKeys();
        }

        final List<Record<Event>> recordsToProcessLocally = peerForwarder.processRecords(records, identificationKeys);

        return innerProcessor.execute(recordsToProcessLocally);
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
